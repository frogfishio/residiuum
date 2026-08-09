//! Bounded off-thread assembly for write-time Recovery Shadows.
//!
//! The authoritative writer copies each already-encoded, frame-aligned cohort
//! into a bounded in-memory queue. One worker owns all staging files and writes
//! them in exact 1 MiB chunks. Seal waits for that segment's ordered `Finish`
//! response before authoritative rename, so every crash-recoverable
//! `*.rsh.dual.tmp` beside pending authority is complete and committed.

use super::{PreparedShadowPublish, ShadowDualStream};
use crate::error::StoreError;
use crate::layout::StorePaths;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

/// Four maximum-size (16 MiB) cohorts: at most 64 MiB queued Shadow payload.
const MAX_QUEUED_COHORTS: usize = 4;

enum Command {
    Begin {
        paths: StorePaths,
        store_id: [u8; 16],
        segment_id: [u8; 16],
        initial: Vec<u8>,
    },
    Append {
        segment_id: [u8; 16],
        chunk: Vec<u8>,
    },
    Finish {
        segment_id: [u8; 16],
        summary: Vec<u8>,
        shard: u16,
        reply: mpsc::Sender<Result<PreparedShadowPublish, StoreError>>,
    },
    Abandon {
        segment_id: [u8; 16],
    },
    Shutdown,
}

struct StageState {
    stream: Option<ShadowDualStream>,
    error: Option<String>,
}

impl StageState {
    fn failed(detail: impl Into<String>) -> Self {
        Self {
            stream: None,
            error: Some(detail.into()),
        }
    }

    fn into_result(self) -> Result<ShadowDualStream, StoreError> {
        self.stream.ok_or_else(|| {
            StoreError::Io(std::io::Error::other(
                self.error
                    .unwrap_or_else(|| "Shadow staging state missing".into()),
            ))
        })
    }
}

/// Store-wide bounded Shadow staging worker.
pub struct ShadowStagePipeline {
    tx: SyncSender<Command>,
    join: Option<JoinHandle<()>>,
}

/// One active segment's ordered handle into the store-wide staging worker.
pub struct ShadowStageHandle {
    tx: SyncSender<Command>,
    segment_id: Option<[u8; 16]>,
}

impl ShadowStagePipeline {
    /// Start one worker shared by all active writer shards.
    pub fn start() -> Self {
        let (tx, rx) = mpsc::sync_channel(MAX_QUEUED_COHORTS);
        let join = thread::Builder::new()
            .name("residiuum-shadow-stage".into())
            .spawn(move || worker_loop(rx))
            .expect("spawn Recovery Shadow staging worker");
        Self {
            tx,
            join: Some(join),
        }
    }

    /// Begin an active segment, seeded from its exact authoritative prefix.
    pub fn begin(
        &self,
        paths: StorePaths,
        store_id: [u8; 16],
        segment_id: [u8; 16],
        initial: Vec<u8>,
    ) -> Result<ShadowStageHandle, StoreError> {
        self.tx
            .send(Command::Begin {
                paths,
                store_id,
                segment_id,
                initial,
            })
            .map_err(channel_closed)?;
        Ok(ShadowStageHandle {
            tx: self.tx.clone(),
            segment_id: Some(segment_id),
        })
    }
}

impl Drop for ShadowStagePipeline {
    fn drop(&mut self) {
        let _ = self.tx.send(Command::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl ShadowStageHandle {
    /// Queue one frame-aligned authoritative cohort. The bounded send applies
    /// protection-first backpressure if Shadow cannot keep up.
    pub fn append(&self, chunk: &[u8]) -> Result<(), StoreError> {
        if chunk.is_empty() {
            return Ok(());
        }
        let segment_id = self.segment_id.ok_or_else(|| {
            StoreError::Io(std::io::Error::other(
                "Shadow stage handle already finished",
            ))
        })?;
        self.tx
            .send(Command::Append {
                segment_id,
                chunk: chunk.to_vec(),
            })
            .map_err(channel_closed)
    }

    /// Drain all prior appends for this segment and return complete staging.
    pub fn finish(
        mut self,
        summary: &[u8],
        shard: u16,
    ) -> Result<PreparedShadowPublish, StoreError> {
        let segment_id = self.segment_id.take().ok_or_else(|| {
            StoreError::Io(std::io::Error::other(
                "Shadow stage handle already finished",
            ))
        })?;
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(Command::Finish {
                segment_id,
                summary: summary.to_vec(),
                shard,
                reply: reply_tx,
            })
            .map_err(channel_closed)?;
        reply_rx.recv().map_err(recv_closed)?
    }
}

impl Drop for ShadowStageHandle {
    fn drop(&mut self) {
        if let Some(segment_id) = self.segment_id.take() {
            let _ = self.tx.send(Command::Abandon { segment_id });
        }
    }
}

fn channel_closed<T>(_: mpsc::SendError<T>) -> StoreError {
    StoreError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "Recovery Shadow staging worker gone",
    ))
}

fn recv_closed(_: mpsc::RecvError) -> StoreError {
    StoreError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "Recovery Shadow staging reply lost",
    ))
}

fn worker_loop(rx: Receiver<Command>) {
    let mut stages: HashMap<[u8; 16], StageState> = HashMap::new();
    while let Ok(command) = rx.recv() {
        match command {
            Command::Begin {
                paths,
                store_id,
                segment_id,
                initial,
            } => {
                let state = match ShadowDualStream::begin(&paths, store_id, segment_id).and_then(
                    |mut stream| {
                        stream.append_image_chunk(&initial)?;
                        Ok(stream)
                    },
                ) {
                    Ok(stream) => StageState {
                        stream: Some(stream),
                        error: None,
                    },
                    Err(error) => StageState::failed(error.to_string()),
                };
                stages.insert(segment_id, state);
            }
            Command::Append { segment_id, chunk } => {
                let Some(state) = stages.get_mut(&segment_id) else {
                    stages.insert(segment_id, StageState::failed("Shadow append before begin"));
                    continue;
                };
                if state.error.is_some() {
                    continue;
                }
                if let Some(stream) = state.stream.as_mut() {
                    if let Err(error) = stream.append_image_chunk(&chunk) {
                        state.error = Some(error.to_string());
                        state.stream = None;
                    }
                }
            }
            Command::Finish {
                segment_id,
                summary,
                shard,
                reply,
            } => {
                let result = stages
                    .remove(&segment_id)
                    .ok_or_else(|| {
                        StoreError::Io(std::io::Error::other("Shadow finish before begin"))
                    })
                    .and_then(StageState::into_result)
                    .and_then(|stream| stream.prepare_async_publish(&summary, shard));
                let _ = reply.send(result);
            }
            Command::Abandon { segment_id } => {
                stages.remove(&segment_id);
            }
            Command::Shutdown => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_format::{
        encode_frame, ActiveSegment, FrameHeader, FrameKind, FrameParts, SafetyLimits, SegmentId,
        WIRE_MAJOR, WIRE_MINOR,
    };
    use tempfile::tempdir;

    #[test]
    fn bounded_worker_preserves_order_and_returns_complete_staging() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [3u8; 16];
        let segment_id = [4u8; 16];
        let ids = SegmentId::new(store_id, segment_id);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        let initial = active.as_bytes().to_vec();
        let body = vec![7u8; 2 * 1024 * 1024];
        let envelope = residiuum_format::EMPTY_ENVELOPE.to_vec();
        let frame = encode_frame(&FrameParts {
            header: FrameHeader {
                wire_major: WIRE_MAJOR,
                wire_minor: WIRE_MINOR,
                frame_kind: FrameKind::ItemEvent.as_u8(),
                flags: Default::default(),
                envelope_len: envelope.len() as u32,
                body_len: body.len() as u64,
                logical_len: body.len() as u64,
                writer_sequence: active.writer_sequence(),
                event_id: [5u8; 16],
            },
            envelope,
            body,
        })
        .unwrap();
        active.append_preencoded_frame(&frame).unwrap();

        let pipeline = ShadowStagePipeline::start();
        let handle = pipeline
            .begin(paths.clone(), store_id, segment_id, initial.clone())
            .unwrap();
        handle.append(&frame).unwrap();
        let prepared = handle.finish(&[], 2).unwrap();
        assert_eq!(prepared.encoded_len, active.len());
        assert_eq!(prepared.staging_write_operations, 3);
        drop(prepared);
    }
}

//! Process/host sampler interface — unavailable metrics never encoded as zero.

use crate::runner::MetricObservation;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessSample {
    pub cpu_time_user_ns: MetricObservation,
    pub cpu_time_system_ns: MetricObservation,
    pub rss_bytes: MetricObservation,
    pub peak_rss_bytes: MetricObservation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostSample {
    pub free_bytes: MetricObservation,
    pub free_inodes: MetricObservation,
    pub load_1m: MetricObservation,
    pub thermal_state: MetricObservation,
}

/// Platform sampler. Implementations must use [`MetricObservation::Unavailable`]
/// when a signal cannot be read — never `0.0` as a stand-in.
pub trait HostSampler {
    fn sample_process(&self) -> ProcessSample;
    fn sample_host(&self) -> HostSample;
}

/// Null adapter: every metric unavailable with an explicit reason.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullHostSampler;

impl HostSampler for NullHostSampler {
    fn sample_process(&self) -> ProcessSample {
        ProcessSample {
            cpu_time_user_ns: MetricObservation::unavailable("not_sampled"),
            cpu_time_system_ns: MetricObservation::unavailable("not_sampled"),
            rss_bytes: MetricObservation::unavailable("not_sampled"),
            peak_rss_bytes: MetricObservation::unavailable("not_sampled"),
        }
    }

    fn sample_host(&self) -> HostSample {
        HostSample {
            free_bytes: MetricObservation::unavailable("not_sampled"),
            free_inodes: MetricObservation::unavailable("not_sampled"),
            load_1m: MetricObservation::unavailable("platform_unsupported"),
            thermal_state: MetricObservation::unavailable("platform_unsupported"),
        }
    }
}

/// Portable sampler: free disk via existing platform helper; CPU/RSS optional.
#[derive(Debug, Clone)]
pub struct PortableHostSampler {
    pub work_path: Option<std::path::PathBuf>,
}

impl HostSampler for PortableHostSampler {
    fn sample_process(&self) -> ProcessSample {
        // No unsafe / no libc: leave process metrics unavailable unless wired later.
        NullHostSampler.sample_process()
    }

    fn sample_host(&self) -> HostSample {
        let mut h = NullHostSampler.sample_host();
        if let Some(ref p) = self.work_path {
            match crate::runner::free_space_bytes(p) {
                Ok(b) => h.free_bytes = MetricObservation::available(b as f64, "bytes"),
                Err(_) => {
                    h.free_bytes = MetricObservation::unavailable("observer_failed");
                }
            }
            match crate::runner::free_space_inodes(p) {
                Ok(Some(i)) => h.free_inodes = MetricObservation::available(i as f64, "count"),
                Ok(None) => {
                    h.free_inodes = MetricObservation::unavailable("platform_unsupported");
                }
                Err(_) => {
                    h.free_inodes = MetricObservation::unavailable("observer_failed");
                }
            }
        }
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_never_uses_zero_for_unavailable() {
        let s = NullHostSampler.sample_host();
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("unavailable"));
        // free_bytes should not be available 0
        match s.free_bytes {
            MetricObservation::Unavailable { .. } => {}
            MetricObservation::Available { value, .. } => {
                panic!("expected unavailable, got available {value}")
            }
        }
    }

    #[test]
    fn portable_can_read_free_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let s = PortableHostSampler {
            work_path: Some(tmp.path().to_path_buf()),
        };
        let h = s.sample_host();
        assert!(h.free_bytes.is_available());
    }
}

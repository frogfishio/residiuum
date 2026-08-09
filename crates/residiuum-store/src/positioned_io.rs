//! Portable explicit-offset file writes for the storage hot path.
//!
//! Positioned writes make file location part of the request instead of mutable
//! cursor state. Unix and Windows use their native positional APIs; the narrow
//! fallback preserves the caller's cursor around a seek/write sequence.

use std::fs::File;
use std::io::{self, ErrorKind};
#[cfg(not(any(unix, windows)))]
use std::io::{Seek, SeekFrom, Write};

/// Write the complete buffer at `offset` without relying on the file cursor.
pub(crate) fn write_all_at(file: &mut File, mut offset: u64, mut bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        while !bytes.is_empty() {
            match file.write_at(bytes, offset) {
                Ok(0) => return Err(io::Error::from(ErrorKind::WriteZero)),
                Ok(written) => {
                    offset = offset.saturating_add(written as u64);
                    bytes = &bytes[written..];
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        return Ok(());
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        while !bytes.is_empty() {
            match file.seek_write(bytes, offset) {
                Ok(0) => return Err(io::Error::from(ErrorKind::WriteZero)),
                Ok(written) => {
                    offset = offset.saturating_add(written as u64);
                    bytes = &bytes[written..];
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        return Ok(());
    }

    #[cfg(not(any(unix, windows)))]
    {
        let cursor = file.stream_position()?;
        let result = (|| {
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(bytes)
        })();
        let restore = file.seek(SeekFrom::Start(cursor));
        result.and(restore.map(|_| ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    fn positioned_write_is_exact_and_cursor_independent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("positioned.bin");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.write_all(b"0123456789").unwrap();
        file.seek(SeekFrom::Start(3)).unwrap();
        write_all_at(&mut file, 5, b"ABC").unwrap();
        assert_eq!(file.stream_position().unwrap(), 3);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"01234ABC89");
    }
}

use std::io::{Read, Seek, SeekFrom, Write, Result as IoResult, ErrorKind, Error as IoError};

/// Represents a segment of a data with read/write/seek capabilities, useful for accessing a part of a file
/// or a buffer, similar to `std::io::Take` but with `Seek` support.
#[allow(unused)]
pub struct Segment<'data, T: Seek>{
    /// Data to be used by the segment.
    data: &'data mut T,

    /// Start position of the segment.
    start: u64,

    /// Size of the segment.
    size: u64,

    /// Current position of the segment.
    pos: u64
}

impl<'data, T: Seek> Segment<'data, T> {
    /// Creates a new segment and moves the pointer to the start position without checking the data real size.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Data to be used by the segment.
    /// * `start` - Start position of the segment.
    /// * `size` - Size of the segment.
    #[allow(unused)]
    pub fn new_unsafe(data: &'data mut T, start: u64, size: u64) -> IoResult<Self> {
        // validate size
        if size < 1 {
            return Err(IoError::new(ErrorKind::InvalidData, "segment size must be greater than 0"));
        }

        // get current pos
        let pos = data.stream_position()?;
        Ok(Self {
            data,
            start,
            size,
            pos
        })
    }

    /// Creates a new segment and moves the pointer to the start position checking the data real size first.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Data to be used by the segment.
    /// * `start` - Start position of the segment.
    /// * `size` - Size of the segment.
    #[allow(unused)]
    pub fn new(data: &'data mut T, start: u64, size: u64) -> IoResult<Self> {
        data.seek(SeekFrom::End(0))?;
        let real_size = data.stream_position()?;
        if real_size < start + size {
            return Err(IoError::new(ErrorKind::InvalidData, "segment size is too large"));
        }
        data.seek(SeekFrom::Start(start))?; 
        Self::new_unsafe(data, start, size)
    }
}

impl<'data, T: Read + Seek> Read for Segment<'data, T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // ensure pos is within the segment
        if self.pos < self.start {
            self.data.seek(SeekFrom::Start(self.start))?;
            self.pos = self.start;
        }
        if self.pos > self.start + self.size {
            return Ok(0);
        }

        // handle read with segment overflow so we don't read past the segment
        if self.pos + buf.len() as u64 > self.start + self.size {
            let sub_buf = &mut buf[0..((self.size + self.start) - self.pos) as usize];
            let read = self.data.read(sub_buf)?;
            self.pos += read as u64;
            return Ok(read)
        }

        // handle normal read
        let read = self.data.read(buf)?;
        self.pos += read as u64;
        Ok(read)
    }
}

impl<'data, T: Write + Seek> Write for Segment<'data, T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // ensure pos is within the segment
        if self.pos < self.start {
            self.data.seek(SeekFrom::Start(self.start))?;
            self.pos = self.start;
        }
        if self.pos > self.start + self.size {
            return Err(IoError::new(ErrorKind::InvalidData, "buffer size is too large for this segment remaining bytes"));
        }

        // handle write with segment overflow so we don't write past the segment
        if self.pos + buf.len() as u64 > self.size {
            return Err(IoError::new(ErrorKind::InvalidData, "buffer size is too large for this segment remaining bytes"));
        }

        // handle normal write
        let written = self.data.write(buf)?;
        self.pos += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.data.flush()
    }
}

impl<'data, T: Seek> Seek for Segment<'data, T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // ensure the position is within the segment
        let real_pos = match pos {
            SeekFrom::Start(pos) => {
                if pos < self.size {
                    SeekFrom::Start(pos + self.start)
                } else {
                    return Err(IoError::new(ErrorKind::InvalidData, "can't seek beyond the segment limit"));
                }
            },
            SeekFrom::End(pos) => if pos > 0 {
                return Err(IoError::new(ErrorKind::InvalidData, "can't seek beyond the segment limit"));
            } else if ((-pos) as u64) < self.size {
                SeekFrom::Start(self.start + self.size -1 - (-pos) as u64)
            } else {
                return Err(IoError::new(ErrorKind::InvalidData, "can't seek before the segment starting position"));
            },
            SeekFrom::Current(pos) => {
                // adjust position to be relative to the segment
                let mut virt_pos: i128 = pos as i128;
                let new_pos = if self.pos < self.start {
                    virt_pos = self.start as i128 + pos as i128 -self.pos as i128;
                    virt_pos
                } else {
                    self.pos as i128 + pos as i128
                };

                // validate position
                if new_pos < self.start as i128 {
                    return Err(IoError::new(ErrorKind::InvalidData, "can't seek before the segment starting position"));
                } else if new_pos < (self.start + self.size) as i128 {
                    SeekFrom::Current(virt_pos as i64)
                } else {
                    return Err(IoError::new(ErrorKind::InvalidData, "can't seek beyond the segment limit"));
                }
            }
        };
        self.pos = self.data.seek(real_pos)?;
        Ok(self.pos - self.start)
    }
}

#[cfg(test)]
mod test_helper {
    pub const SAMPLE_DATA: [u8; 20] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19];
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helper::SAMPLE_DATA;

    #[test]
    fn new() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        let segment = match Segment::new(&mut data, 5, 10) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        assert_eq!(5, segment.pos);
        assert_eq!(10, segment.size);
        assert_eq!(5, segment.start);
        match data.stream_position() {
            Ok(pos) => assert!(pos == 5),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_move_pos_to_start_when_before_segment() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        let segment = match Segment::new(&mut data, 5, 10) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        assert_eq!(5, segment.pos);
        assert_eq!(10, segment.size);
        assert_eq!(5, segment.start);
        match data.stream_position() {
            Ok(pos) => assert_eq!(5, pos),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_move_pos_to_start_when_inside_segment() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(7)).unwrap();
        let segment = match Segment::new(&mut data, 5, 10) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        assert_eq!(5, segment.pos);
        assert_eq!(10, segment.size);
        assert_eq!(5, segment.start);
        match data.stream_position() {
            Ok(pos) => assert_eq!(5, pos),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_invalid_size() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        match Segment::new(&mut data, 6, 1000) {
            Ok(_) => assert!(false, "expected an error but got a segment"),
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("segment size is too large", msg),
                    _ => assert!(false, "expected an invalid data error but got: {}", e),
                }
            },
        };
    }

    #[test]
    fn new_unsafe() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        let segment = match Segment::new_unsafe(&mut data, 100, 1024) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        assert_eq!(0, segment.pos);
        assert_eq!(1024, segment.size);
        assert_eq!(100, segment.start);
        match data.stream_position() {
            Ok(pos) => assert_eq!(0, pos),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_unsafe_size_zero() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        match Segment::new_unsafe(&mut data, 4, 0) {
            Ok(_) => assert!(false, "expected an error but got a segment"),
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("segment size must be greater than 0", msg),
                    _ => assert!(false, "expected an invalid data error but got: {}", e),
                }
            },
        };
    }

    #[test]
    fn read() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 5, 10) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 4];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 4);
        assert_eq!(buf, [5, 6, 7, 8]);
        assert_eq!(data.position(), 9);
    }

    #[test]
    fn read_past_end() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        let mut segment = match Segment::new_unsafe(&mut data, 7, 3) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 5];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 3);
        assert_eq!(buf, [7, 8, 9, 0, 0]);
        assert_eq!(data.position(), 10);
    }

    #[test]
    fn read_past_end_with_offset() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(7)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 5, 5) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 5];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 3);
        assert_eq!(buf, [7, 8, 9, 0, 0]);
        assert_eq!(data.position(), 10);
    }

    #[test]
    fn read_within_segment_negative_offset() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 5, 10) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 5];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 5);
        assert_eq!(buf, [5, 6, 7, 8, 9]);
        assert_eq!(data.position(), 10);
    }

    #[test]
    fn read_within_segment_with_offset() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(4)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 2, 10) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 5];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 5);
        assert_eq!(buf, [4, 5, 6, 7, 8]);
        assert_eq!(data.position(), 9);
    }

    #[test]
    fn read_after_end_with_offset() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(10)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 5, 3) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 5];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 0);
        assert_eq!(buf, [0, 0, 0, 0, 0]);
        assert_eq!(data.position(), 10);
    }

    #[test]
    fn read_after_end_without_offset() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(8)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 5, 3) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        let mut buf = vec![0; 5];
        let read = match segment.read(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a read but got an error: {}", e);
                return;
            },
        };
        assert_eq!(read, 0);
        assert_eq!(buf, [0, 0, 0, 0, 0]);
        assert_eq!(data.position(), 8);
    }

    #[test]
    fn write() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10).unwrap();
        let buf = [5, 6, 7, 8, 9];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        assert_eq!(data.position(), 10);
        let data = data.into_inner();
        let expected = [0, 0, 0, 0, 0, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_past_end() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(8)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 3).unwrap();
        let buf = [1, 2, 3, 4, 5];
        match segment.write(&buf) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("buffer size is too large for this segment remaining bytes", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        };
        let data = data.into_inner();
        let expected = [0u8; 20];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_past_end_with_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(7)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        let buf = [7, 8, 9, 10, 11];
        match segment.write(&buf) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("buffer size is too large for this segment remaining bytes", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        };
        let data = data.into_inner();
        let expected = [0u8; 20];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_within_segment_negative_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 11).unwrap();
        let buf = [5, 6, 7, 8, 9];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        assert_eq!(data.position(), 10);
        let data = data.into_inner();
        let expected = [0, 0, 0, 0, 0, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_within_segment_with_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(4)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 2, 10).unwrap();
        let buf = [4, 5, 6, 7, 8];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        let data = data.into_inner();
        let expected = [0, 0, 0, 0, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_after_end_with_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(10)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 3).unwrap();
        let buf = [1, 2, 3, 4, 5];
        match segment.write(&buf) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("buffer size is too large for this segment remaining bytes", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        };
        let actual = data.into_inner();
        let expected = [0u8; 20];
        assert_eq!(actual, expected, "Data array should remain unchanged");
    }

    #[test]
    fn write_after_end_without_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(8)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 3).unwrap();
        let buf = [1, 2, 3, 4, 5];
        match segment.write(&buf) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("buffer size is too large for this segment remaining bytes", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        };
        let actual = data.into_inner();
        let expected = [0u8; 20];
        assert_eq!(actual, expected, "Data array should remain unchanged");
    }

    #[test]
    fn seek_current() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10).unwrap();
        let pos = segment.seek(SeekFrom::Current(5)).unwrap();
        assert_eq!(pos, 5);
        assert_eq!(data.position(), 10);
    }

    #[test]
    fn seek_current_position_adjustment_positive() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10).unwrap();
        let pos = segment.seek(SeekFrom::Current(2)).unwrap();
        assert_eq!(pos, 2);
        assert_eq!(data.position(), 7);
    }

    #[test]
    fn seek_current_position_adjustment_negative() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10).unwrap();
        match segment.seek(SeekFrom::Current(-1)) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("can't seek before the segment starting position", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        }
    }

    #[test]
    fn seek_current_overflow() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        match segment.seek(SeekFrom::Current(6)) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("can't seek beyond the segment limit", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        }
    }

    #[test]
    fn seek_current_before_start() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(7)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        match segment.seek(SeekFrom::Current(-3)) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("can't seek before the segment starting position", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        }
    }

    #[test]
    fn seek_start() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10).unwrap();
        let pos = segment.seek(SeekFrom::Start(3)).unwrap();
        assert_eq!(pos, 3);
        assert_eq!(data.position(), 8);
    }

    #[test]
    fn seek_start_overflow() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        match segment.seek(SeekFrom::Start(6)) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("can't seek beyond the segment limit", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        }
    }

    #[test]
    fn seek_end() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        let pos = segment.seek(SeekFrom::End(-3)).unwrap();
        assert_eq!(pos, 1);
        assert_eq!(data.position(), 6);
    }

    #[test]
    fn seek_end_before_start() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        match segment.seek(SeekFrom::End(-6)) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("can't seek before the segment starting position", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        }
    }

    #[test]
    fn seek_end_after_end() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5).unwrap();
        match segment.seek(SeekFrom::End(1)) {
            Ok(_) => {
                assert!(false, "expected an error but got Ok");
                return;
            },
            Err(e) => {
                let msg = e.to_string();
                match e.kind() {
                    ErrorKind::InvalidData => assert_eq!("can't seek beyond the segment limit", msg),
                    _ => {
                        assert!(false, "expected an invalid data error but got: {}", e);
                        return;
                    },
                }
            },
        }
    }
}
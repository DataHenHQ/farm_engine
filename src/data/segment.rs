use std::io::{Read, Seek, SeekFrom, Write, Result as IoResult, ErrorKind, Error as IoError};

/// Represents a segment of a data with read/write/seek capabilities, useful for accessing a part of a file
/// or a buffer, similar to `std::io::Take` but with `Seek` support.
#[derive(PartialEq, Debug)]
pub struct Segment<'data, T: Seek>{
    /// Data to be used by the segment.
    data: &'data mut T,

    /// Start position of the segment.
    start: u64,

    /// Size of the segment.
    size: u64,

    /// Current position of the segment.
    pos: u64,

    /// Real size of the data.
    real_size: u64,

    /// Whether to allow growing the segment.
    pub allow_grow: bool
}

impl<'data, T: Seek> Segment<'data, T> {
    /// Creates a new segment without any internal checks.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Data to be used by the segment.
    /// * `start` - Start position of the segment.
    /// * `segment_size` - Size of the segment.
    /// * `pos` - Current position of the segment.
    /// * `data_size` - Real size of the data.
    /// * `allow_grow` - Whether to allow growing the segment size.
    pub fn inner_new_unsafe(data: &'data mut T, start: u64, segment_size: u64, pos: u64, data_size: u64, allow_grow: bool) -> IoResult<Self> {
        // validate size
        if !allow_grow && segment_size < 1 {
            return Err(IoError::new(ErrorKind::InvalidData, "segment size must be greater than 0"));
        }
        Ok(Self {
            data,
            start,
            size: segment_size,
            pos,
            real_size: data_size,
            allow_grow
        })
    }

    /// Creates a new segment and moves the pointer to the start position without checking the data real size,
    /// this allows to increase the real data size (if data isn't static) as long as it remains within the
    /// segment bounds or unless allowed to grow.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Data to be used by the segment.
    /// * `start` - Start position of the segment.
    /// * `segment_size` - Size of the segment.
    /// * `data_size` - Real size of the data.
    /// * `allow_grow` - Whether to allow growing the segment defined size (if data isn't static).
    pub fn new_unsafe(data: &'data mut T, start: u64, segment_size: u64, data_size: u64, allow_grow: bool) -> IoResult<Self> {
        let mut pos = data.stream_position()?;
        if pos < start {
            // force start position when the current position is under the start position
            // due no negative seek positions rule
            pos = data.seek(SeekFrom::Start(start.max(pos)))?;
        }
        Self::inner_new_unsafe(data, start, segment_size, pos, data_size, allow_grow)
    }

    /// Creates a new segment and moves the pointer to the start position checking the data real size first so
    /// the segment bounds can't exceed the real data size unless it is allowed to grow.
    /// 
    /// # Arguments
    /// 
    /// * `data` - Data to be used by the segment.
    /// * `start` - Start position of the segment.
    /// * `size` - Size of the segment.
    /// * `allow_grow` - Whether to allow growing the segment size (if data isn't static). An error is
    ///   triggered when the segment size overflows the real size.
    pub fn new(data: &'data mut T, start: u64, size: u64, allow_grow: bool) -> IoResult<Self> {
        let real_size = data.seek(SeekFrom::End(0))?;
        if !allow_grow && real_size < start + size {
            return Err(IoError::new(ErrorKind::InvalidData, "segment size is too large"));
        }
        let pos = data.seek(SeekFrom::Start(start))?; 
        Self::inner_new_unsafe(data, start, size, pos, real_size, allow_grow)
    }
}

impl<'data, T: Read + Seek> Read for Segment<'data, T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // ensure pos is within the segment
        if self.pos >= self.start + self.size {
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
        if !self.allow_grow && self.pos >= self.start + self.size {
            return Ok(0);
        }

        // handle write with segment overflow so we don't write past the segment
        let buf_len = buf.len() as u64;
        let written = if !self.allow_grow && self.pos + buf_len > self.start + self.size {
            self.data.write(&buf[0..(self.start + self.size - self.pos) as usize])?
        } else {
            self.data.write(buf)?
        };
        self.pos += written as u64;
        if self.allow_grow && self.pos > self.start + self.size {
            self.size = self.pos - self.start;
            if self.pos > self.real_size {
                self.real_size = self.pos;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.data.flush()
    }
}

impl<'data, T: Seek> Seek for Segment<'data, T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // ensure the position is within the segment
        let real_seek_from = match pos {
            SeekFrom::Start(pos) => {
                // handle overflow
                if pos > self.size {
                    self.pos = self.start + pos;
                    return Ok(pos);
                }
                SeekFrom::Start(pos + self.start)
            },
            SeekFrom::End(pos) => {
                let size = self.real_size.min(self.size + self.start) - self.start;
                // handle overflow
                if pos > 0 {
                    let new_pos = self.start + size + pos as u64;
                    // handle real_size < size
                    if new_pos > size + self.start {
                        self.pos = new_pos;
                        return Ok(self.pos - self.start);
                    }
                    SeekFrom::Start(new_pos - self.start)
                } else {
                    // handle underflow
                    if ((-pos) as u64) > size {
                        return Err(IoError::new(ErrorKind::InvalidData, "can't seek before the segment starting position"));
                    }
                    SeekFrom::Start(self.start + size - (-pos) as u64)
                }
            },
            SeekFrom::Current(pos) => {
                if pos > 0 {
                    // handle overflow
                    if self.pos + pos as u64 > self.start + self.size {
                        self.pos = self.pos + pos as u64;
                        return Ok(self.pos - self.start);
                    }
                } else if ((-pos) as u64) > self.pos - self.start {
                    // handle underflow
                    return Err(IoError::new(ErrorKind::InvalidData, "can't seek before the segment starting position"));
                }
                SeekFrom::Current(pos)
            }
        };
        self.pos = self.data.seek(real_seek_from)?;
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
        let segment = match Segment::new(&mut data, 5, 10, false) {
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
            Ok(pos) => assert_eq!(pos, 5),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_with_allow_grow() {
        let mut data = std::io::Cursor::new(Vec::new());
        data.write_all(&SAMPLE_DATA).unwrap();
        let segment = match Segment::new(&mut data, 5, 10, true) {
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
            Ok(pos) => assert_eq!(pos, 5),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_with_allow_grow_overflow() {
        let mut data = std::io::Cursor::new(Vec::new());
        data.write_all(&SAMPLE_DATA).unwrap();
        let segment = match Segment::new(&mut data, 5, 25, true) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        segment.data.seek(SeekFrom::End(0)).unwrap();
        let real_size = segment.data.stream_position().unwrap() - segment.start;
        segment.data.seek(SeekFrom::Start(segment.start)).unwrap();
        assert_eq!(real_size, 15);
        assert_eq!(5, segment.pos);
        assert_eq!(25, segment.size);
        assert_eq!(5, segment.start);
        match data.stream_position() {
            Ok(pos) => assert_eq!(pos, 5),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_move_pos_to_start_when_before_segment() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        let segment = match Segment::new(&mut data, 5, 10, false) {
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
        let segment = match Segment::new(&mut data, 5, 10, false) {
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
        match Segment::new(&mut data, 6, 1000, false) {
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
        let segment = match Segment::new_unsafe(&mut data, 100, 1024, SAMPLE_DATA.len() as u64, false) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        assert_eq!(100, segment.pos);
        assert_eq!(1024, segment.size);
        assert_eq!(100, segment.start);
        match data.stream_position() {
            Ok(pos) => assert_eq!(100, pos),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn new_unsafe_size_zero() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        match Segment::new_unsafe(&mut data, 4, 0, SAMPLE_DATA.len() as u64, false) {
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
    fn new_unsafe_allow_grow_size_zero() {
        let mut data = std::io::Cursor::new(Vec::new());
        data.write_all(&SAMPLE_DATA).unwrap();
        let segment = match Segment::new_unsafe(&mut data, 4, 0, 0, true) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a new segment but got an error: {}", e);
                return;
            },
        };
        segment.data.seek(SeekFrom::End(0)).unwrap();
        let real_size = segment.data.stream_position().unwrap() - segment.start;
        assert_eq!(real_size, 16);
        assert_eq!(20, segment.pos);
        assert_eq!(0, segment.size);
        assert_eq!(4, segment.start);
        match data.stream_position() {
            Ok(pos) => assert_eq!(pos, 20),
            Err(e) => {
                assert!(false, "expected a data position but got an error: {}", e);
                return;
            },
        }
    }

    #[test]
    fn read() {
        let mut data = std::io::Cursor::new(SAMPLE_DATA);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = match Segment::new_unsafe(&mut data, 5, 10, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = match Segment::new_unsafe(&mut data, 7, 3, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = match Segment::new_unsafe(&mut data, 5, 5, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = match Segment::new_unsafe(&mut data, 5, 10, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = match Segment::new_unsafe(&mut data, 2, 10, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = match Segment::new_unsafe(&mut data, 5, 3, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = match Segment::new_unsafe(&mut data, 5, 3, SAMPLE_DATA.len() as u64, false) {
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
        let mut segment = Segment::new_unsafe(&mut data, 5, 10, 20, false).unwrap();
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
        let mut segment = Segment::new_unsafe(&mut data, 5, 3, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 8);
        assert_eq!(segment.pos, 8);
        let buf = [1, 2, 3, 4, 5];
        match segment.write(&buf) {
            Ok(written) => {
                assert_eq!(written, 0);
                assert_eq!(segment.data.stream_position().unwrap(), 8);
                assert_eq!(segment.size, 3);
                assert_eq!(segment.pos, 8);
            },
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        let data = data.into_inner();
        let expected = [0u8; 20];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_allow_grow_past_end() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(8)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 3, 20, true).unwrap();
        let buf = [8, 9, 10, 11, 12];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        assert_eq!(segment.data.stream_position().unwrap(), 13);
        assert_eq!(segment.size, 8);
        assert_eq!(segment.pos, 13);
        let data = data.into_inner();
        let expected = [0, 0, 0, 0, 0, 0, 0, 0, 8, 9, 10, 11, 12, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_past_end_with_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(7)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 7);
        assert_eq!(segment.pos, 7);
        let buf = [7, 8, 9, 10, 11];
        match segment.write(&buf) {
            Ok(written) => {
                assert_eq!(written, 3);
                assert_eq!(segment.data.stream_position().unwrap(), 10);
                assert_eq!(segment.size, 5);
                assert_eq!(segment.pos, 10);
            },
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        let data = data.into_inner();
        let expected = [0, 0, 0, 0, 0, 0, 0, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_past_allow_grow_end_with_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(7)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, true).unwrap();
        let buf = [7, 8, 9, 10, 11];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        assert_eq!(segment.data.stream_position().unwrap(), 12);
        assert_eq!(segment.size, 7);
        assert_eq!(segment.pos, 12);
        let data = data.into_inner();
        let expected = [0, 0, 0, 0, 0, 0, 0, 7, 8, 9, 10, 11, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_within_segment_negative_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 11, 20, false).unwrap();
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
        let mut segment = Segment::new_unsafe(&mut data, 2, 10, 20, false).unwrap();
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
    fn write_real_grow_within_offset() {
        let mut data = std::io::Cursor::new(vec![0u8; 20]);
        data.seek(SeekFrom::Start(18)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 18, 10, 20, false).unwrap();
        let buf = [18, 19, 20, 21, 22];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        assert_eq!(segment.data.stream_position().unwrap(), 23);
        assert_eq!(segment.size, 10);
        assert_eq!(segment.pos, 23);
        let data = data.into_inner();
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 19, 20, 21, 22];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_allow_grow_real_grow() {
        let mut data = std::io::Cursor::new(vec![0u8; 20]);
        data.seek(SeekFrom::Start(18)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 18, 1, 20, true).unwrap();
        let buf = [18, 19, 20, 21, 22];
        let written = match segment.write(&buf) {
            Ok(v) => v,
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        assert_eq!(written, 5);
        assert_eq!(segment.data.stream_position().unwrap(), 23);
        assert_eq!(segment.size, 5);
        assert_eq!(segment.pos, 23);
        let data = data.into_inner();
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 19, 20, 21, 22];
        assert_eq!(expected, data);
    }

    #[test]
    fn write_after_end_with_offset() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(10)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 3, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 10);
        assert_eq!(segment.pos, 10);
        let buf = [1, 2, 3, 4, 5];
        match segment.write(&buf) {
            Ok(written) => {
                assert_eq!(written, 0);
                assert_eq!(segment.data.stream_position().unwrap(), 10);
                assert_eq!(segment.size, 3);
                assert_eq!(segment.pos, 10);
            },
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
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
        let mut segment = Segment::new_unsafe(&mut data, 5, 3, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 8);
        assert_eq!(segment.pos, 8);
        let buf = [1, 2, 3, 4, 5];
        match segment.write(&buf) {
            Ok(written) => {
                assert_eq!(written, 0);
                assert_eq!(segment.data.stream_position().unwrap(), 8);
                assert_eq!(segment.size, 3);
                assert_eq!(segment.pos, 8);
            },
            Err(e) => {
                assert!(false, "expected a write but got an error: {}", e);
                return;
            },
        };
        let actual = data.into_inner();
        let expected = [0u8; 20];
        assert_eq!(actual, expected, "Data array should remain unchanged");
    }

    #[test]
    fn seek_current() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        let mut segment = Segment::new_unsafe(&mut data, 5, 10, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        let pos = segment.seek(SeekFrom::Current(5)).unwrap();
        assert_eq!(pos, 5);
        assert_eq!(data.stream_position().unwrap(), 10);
    }

    #[test]
    fn seek_current_position_adjustment_positive() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10, 20, false).unwrap();
        let pos = segment.seek(SeekFrom::Current(2)).unwrap();
        assert_eq!(pos, 2);
        assert_eq!(data.position(), 7);
    }

    #[test]
    fn seek_current_position_adjustment_negative() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(8)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 8);
        assert_eq!(segment.pos, 8);
        match segment.seek(SeekFrom::Current(-2)) {
            Ok(pos) => {
                assert_eq!(pos, 1);
                assert_eq!(segment.data.stream_position().unwrap(), 6);
                assert_eq!(segment.pos, 6);
            },
            Err(e) => assert!(false, "expected success but got error: {}", e),
        }
    }

    #[test]
    fn seek_current_position_adjustment_negative_before_start() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(2)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 10, 20, false).unwrap();
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
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        match segment.seek(SeekFrom::Current(6)) {
            Ok(pos) => {
                assert_eq!(pos, 6);
                assert_eq!(segment.data.stream_position().unwrap(), 5);
            },
            Err(e) => {
                assert!(false, "expected success but got error: {}", e);
            },
        }
    }

    #[test]
    fn seek_current_before_start() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(7)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
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
        let mut segment = Segment::new_unsafe(&mut data, 5, 10, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        assert_eq!(segment.pos, 5);
        let pos = segment.seek(SeekFrom::Start(3)).unwrap();
        assert_eq!(pos, 3);
        assert_eq!(data.position(), 8);
    }

    #[test]
    fn seek_start_overflow() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        assert_eq!(segment.pos, 5);
        match segment.seek(SeekFrom::Start(6)) {
            Ok(pos) => {
                assert_eq!(pos, 6);
                assert_eq!(segment.data.stream_position().unwrap(), 5);
                assert_eq!(segment.pos, 11);
            },
            Err(e) => assert!(false, "expected a seek but got an error: {}", e),
        }
    }

    #[test]
    fn seek_end() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        assert_eq!(segment.pos, 5);
        let pos = segment.seek(SeekFrom::End(-3)).unwrap();
        assert_eq!(pos, 2);
        assert_eq!(segment.data.stream_position().unwrap(), 7);
        assert_eq!(segment.pos, 7);
    }

    #[test]
    fn seek_end_before_start() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        data.seek(SeekFrom::Start(5)).unwrap();
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
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
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 20, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        assert_eq!(segment.pos, 5);
        match segment.seek(SeekFrom::End(1)) {
            Ok(pos) => {
                assert_eq!(pos, 6);
                assert_eq!(segment.data.stream_position().unwrap(), 5);
                assert_eq!(segment.pos, 11);
            },
            Err(e) => assert!(false, "expected a seek but got an error: {}", e),
        }
    }

    #[test]
    fn seek_end_size_not_real_size() {
        let mut data = std::io::Cursor::new([0u8; 20]);
        let mut segment = Segment::new_unsafe(&mut data, 5, 5, 8, false).unwrap();
        assert_eq!(segment.data.stream_position().unwrap(), 5);
        assert_eq!(segment.pos, 5);
        match segment.seek(SeekFrom::End(0)) {
            Ok(pos) => {
                assert_eq!(pos, 3);
                assert_eq!(segment.data.stream_position().unwrap(), 8);
                assert_eq!(segment.pos, 8);
            },
            Err(e) => assert!(false, "expected a seek but got an error: {}", e),
        }
    }
}
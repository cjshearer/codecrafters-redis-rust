use crate::frame::{Bool, Frame, InvalidBool, InvalidPrefix, Prefix};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use core::num;
use std::{
    io::{
        self,
        ErrorKind::{self, UnexpectedEof},
    },
    str,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// Resources I found helpful while making this:
// https://en.wikipedia.org/wiki/Berkeley_sockets
// https://fasterthanli.me/articles/pin-and-suffering
// https://itnext.io/rust-the-joy-of-safe-zero-copy-parsers-8c8581db8ab2
// https://blog.burntsushi.net/rust-error-handling/
// https://www.youtube.com/watch?v=UfMOOxOGCmA

/// A wrapper over a stream, used to read and write RESP frames.
pub struct Connection<'a, RW> {
    read_buf: BytesMut,
    write_buf: BytesMut,
    stream: &'a mut RW,
}

const DEFAULT_BUF_SIZE: usize = 4096;
const LF: u8 = b'\n';
const CRLF: &[u8] = &[b'\r', LF];

impl<'a, RW: AsyncRead + AsyncWrite + Unpin> Connection<'a, RW> {
    /// Creates a new Connection with a default read/write buffer capacity. The default is currently
    /// 4 KB.
    ///
    /// Note that the buffer will implicitly grow as needed.
    pub fn new(stream: &'a mut RW) -> Self {
        Self::with_capacity(DEFAULT_BUF_SIZE, DEFAULT_BUF_SIZE, stream)
    }

    /// Creates a new Connection with the specified buffer capacity.
    pub fn with_capacity(read_capacity: usize, write_capacity: usize, stream: &'a mut RW) -> Self {
        Self {
            read_buf: BytesMut::with_capacity(read_capacity),
            write_buf: BytesMut::with_capacity(write_capacity),
            stream,
        }
    }

    /// Reads and parses a frame from the underlying stream, returning:
    /// - `Ok(None)`, if the buffer begins with an Eof
    /// - `Ok(Some(frame))`, if the buffer begins with a complete frame
    /// - `Err(...)`, if the buffer begins with an invalid frame
    ///
    /// Two points should be noted about this method's implementation:
    ///
    /// 1. While reading and parsing are normally separated, they have been merged here to avoid
    ///    validating that a complete frame has been buffered, which would:
    ///     - incur the overhead of checking that we are ready to parse
    ///     - violate "parse, don't validate"
    ///
    /// 2. This method implements zero-copy parsing, which requires storing the data in a special
    ///    buffer that can be partitioned off without reallocation. Bytes are read into a `BytesMut`
    ///    buffer from which references are `split_off` into a frame. While tokio provides an async
    ///    buffered reader, it mirrors the std library's usage of a `vec`-backed buffer, and so is
    ///    limited to unique ownership of the allocated memory, which unlike `BytesMut`, cannot be
    ///    split without reallocating.
    ///
    /// So why not read directly from the stream into the Frame? That would trivially avoid copying,
    /// without all this complexity. From what I can tell, there a two reasons:
    ///
    /// 1. RESP frame headers don't always provide the length of the frame we need to read (besides,
    ///    the headers themselves are dynamically sized). As the Berkley Sockets API (specifically,
    ///    `recv()`) is only designed to read up to n bytes into a buffer, we must blindly keep
    ///    reading chunks of bytes until we have enough. Eventually, we will "accidentally" read
    ///    bytes associated with the next frame, and once we've read them, we have to store them
    ///    somewhere, so we store them in an intermediate buffer. As for reading one byte at a
    ///    time:...  
    ///
    /// 2. Even if RESP frames were all length-prefixed, attempting to read only the bytes we
    ///    immediately need would be excessively inefficient. Syscalls take relatively far more time
    ///    than than other operations, so when a socket is ready to be read, it's generally best to
    ///    read everything it has.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ReadError> {
        let mut array_stack: Vec<(Vec<Frame>, usize)> = vec![];

        loop {
            // fold completed arrays into previous ones or return the last one if it is completed
            while let Some((complete_array, _)) = array_stack
                .last()
                .is_some_and(|(arr, intended_capacity)| arr.len() == *intended_capacity)
                .then(|| array_stack.pop().unwrap())
            {
                let frame = Frame::Array(complete_array);
                if array_stack.len() == 0 {
                    return Ok(Some(frame));
                }
                array_stack.last_mut().unwrap().0.push(frame);
            }

            let prefix: Prefix = match self.read_u8().await {
                Err(e) if array_stack.is_empty() && e.kind() == UnexpectedEof => return Ok(None),
                r => r?.try_into()?,
            };

            let mut payload = self.read_line().await?;
            if CRLF != payload.split_off(payload.len() - 2) {
                return Err(ReadError::MissingTerminator);
            }

            let frame = match prefix {
                Prefix::Array => {
                    let size = str::from_utf8(&payload)?.parse()?;
                    let array: Vec<Frame> = Vec::with_capacity(size);
                    if array.capacity() != 0 {
                        array_stack.push((array, size));
                        continue;
                    }
                    Frame::Array(array)
                }
                Prefix::Boolean => Frame::Boolean(Bool::try_from(payload.as_ref())?.into()),
                Prefix::Bulk => {
                    let size = str::from_utf8(&payload)?.parse::<usize>()? + 2;
                    let mut data = self.read_exact(size).await?;
                    if CRLF != data.split_off(data.len() - 2) {
                        return Err(ReadError::MissingTerminator);
                    }
                    Frame::Bulk(data)
                }
                Prefix::Error => Frame::Error(payload),
                Prefix::Integer => Frame::Integer(str::from_utf8(&payload)?.parse()?),
                Prefix::Null => Frame::Null,
                Prefix::String => Frame::String(payload),
            };

            if let Some((current_array, _)) = array_stack.last_mut() {
                current_array.push(frame);
            } else {
                return Ok(Some(frame));
            }
        }
    }

    // TODO(cjshearer): if I ever get around to benchmarking this, it would be cool to see if this
    // could be optimized in the case of large, non-array type frames. If mem::size_of(frame)
    // crosses some threshold, then skipping the intermediate buffer and writing each part of the
    // frame directly to the stream could be faster.
    pub async fn write_frame(&mut self, frame: Frame) -> io::Result<()> {
        let init_arr = [frame];
        let mut iter_stack = vec![init_arr.iter()];
        while let Some((frame, remaining)) = iter_stack
            .last_mut()
            .and_then(|iter| iter.next().map(|f| (f, iter.len())))
        {
            if 0 == remaining {
                iter_stack.pop();
            }
            self.write_buf.put_u8(frame.prefix());
            match frame {
                Frame::Array(array) => {
                    self.write_buf
                        .put_slice(&array.len().to_string().as_bytes());
                    if array.len() > 0 {
                        iter_stack.push(array.iter());
                    }
                }
                Frame::Boolean(bool) => self.write_buf.put_u8(Bool::from(*bool).into()),
                Frame::Bulk(bulk) => {
                    self.write_buf.put_slice(bulk.len().to_string().as_bytes());
                    self.write_buf.put_slice(CRLF);
                    self.write_buf.put_slice(bulk.as_ref());
                }
                Frame::Error(error) => self.write_buf.put_slice(error.as_ref()),
                Frame::Integer(i) => self.write_buf.put_slice(&i.to_string().as_bytes()),
                Frame::Null => (),
                Frame::String(string) => self.write_buf.put_slice(string.as_ref()),
            };
            self.write_buf.put_slice(CRLF);
        }
        self.stream.write_all_buf(&mut self.write_buf).await
    }

    /// Reads more than 0 bytes into the read_buffer, returning an EoF error if none could be read
    async fn must_fill_buf(&mut self) -> io::Result<usize> {
        return match self.stream.read_buf(&mut self.read_buf).await? {
            0 => Err(UnexpectedEof.into()),
            s => Ok(s),
        };
    }

    /// Reads all bytes until a newline (the 0xA byte) is reached, returning them as `Bytes`.
    async fn read_line(&mut self) -> io::Result<Bytes> {
        let mut cursor = 0;
        loop {
            if let Some(terminal) = self.read_buf[cursor..].iter().position(|c| *c == LF) {
                cursor = terminal;
                break;
            }
            cursor = self.read_buf.len();
            self.must_fill_buf().await?;
        }
        Ok(self.read_buf.split_to(cursor + 1).freeze())
    }

    /// Fills the buffer with at least `size` bytes, returning them as `Bytes`.
    async fn read_exact(&mut self, size: usize) -> io::Result<Bytes> {
        while self.read_buf.len() < size {
            self.must_fill_buf().await?;
        }
        Ok(self.read_buf.split_to(size).freeze())
    }

    /// Reads a u8 from the buffer, filling it with more bytes from the reader if necessary
    async fn read_u8(&mut self) -> io::Result<u8> {
        if !self.read_buf.has_remaining() {
            self.must_fill_buf().await?;
        }
        return Ok(self.read_buf.get_u8());
    }
}

#[derive(Debug, PartialEq)]
pub enum ReadError {
    InvalidBool,
    InvalidPrefix,
    IoError(ErrorKind),
    MissingTerminator,
    ParseIntError(num::ParseIntError),
    Utf8Error(std::str::Utf8Error),
}

impl From<num::ParseIntError> for ReadError {
    fn from(value: num::ParseIntError) -> Self {
        ReadError::ParseIntError(value)
    }
}

impl From<std::io::Error> for ReadError {
    fn from(value: std::io::Error) -> Self {
        ReadError::IoError(value.kind())
    }
}

impl From<std::str::Utf8Error> for ReadError {
    fn from(value: std::str::Utf8Error) -> Self {
        ReadError::Utf8Error(value)
    }
}

impl From<InvalidPrefix> for ReadError {
    fn from(_: InvalidPrefix) -> Self {
        ReadError::InvalidPrefix
    }
}

impl From<InvalidBool> for ReadError {
    fn from(_: InvalidBool) -> Self {
        ReadError::InvalidBool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    macro_rules! read_tests {
        ($($read_test:ident: $bytes:expr => $frame_or_err:expr),*) => {
            $(
                #[tokio::test]
                async fn $read_test() {
                    assert_eq!(
                        $frame_or_err,
                        Connection::new(&mut Cursor::new($bytes.to_vec())).read_frame().await,
                    );
                }
            )*
        };
    }

    macro_rules! test_reading_and_writing_frames {
        ($($read_test:ident: $bytes:expr, $write_test:ident: $frame:expr),*) => {
            $(
                read_tests!{$read_test: $bytes => Ok(Some($frame))}

                #[tokio::test]
                async fn $write_test() {
                    let mut cursor = Cursor::new(Vec::new());
                    let _ = Connection::new(&mut cursor).write_frame($frame).await;
                    assert_eq!($bytes, cursor.into_inner().as_slice());
                }

            )*
        };
    }

    read_tests! {
        invalid_bool: b"#invalid\r\n" => Err(ReadError::InvalidBool),
        missing_terminator: b"+I forgot the trailing CRLF" => Err(ReadError::IoError(UnexpectedEof)),
        read_empty_buffer: b"" => Ok(None)
    }

    test_reading_and_writing_frames! {
        read_string: b"+string\r\n",
        write_string: Frame::String("string".into()),
        read_error: b"-error\r\n",
        write_error: Frame::Error("error".into()),
        read_integer: b":42\r\n",
        write_integer: Frame::Integer(42),
        read_negative_integer: b":-42\r\n",
        write_negative_integer: Frame::Integer(-42),
        read_bulk: b"$4\r\nbulk\r\n",
        write_bulk: Frame::Bulk("bulk".into()),
        read_array: b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        write_array: {
            Frame::Array(vec![
                Frame::Bulk("set".into()),
                Frame::Bulk("key".into()),
                Frame::Bulk("value".into())
            ])
        },
        read_nested_arrays: b"*1\r\n*2\r\n*0\r\n*0\r\n",
        write_nested_arrays: {
            Frame::Array(vec![
                Frame::Array(vec![
                    Frame::Array(vec![]),
                    Frame::Array(vec![]),
                ]),
            ])
        },
        read_null: b"_\r\n",
        write_null: Frame::Null,
        read_true: b"#t\r\n",
        write_true: Frame::Boolean(true),
        read_false: b"#f\r\n",
        write_false: Frame::Boolean(false)
    }

    #[tokio::test]
    async fn multiple_frames() {
        let mut buf = Cursor::new(b"+first frame\r\n+second frame\r\n".to_vec());
        let mut stream = Connection::new(&mut buf);
        assert_eq!(
            Ok(Some(Frame::String("first frame".into()))),
            stream.read_frame().await
        );
        assert_eq!(
            Ok(Some(Frame::String("second frame".into()))),
            stream.read_frame().await
        );
    }
}

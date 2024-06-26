use crate::frame::Frame;
use bytes::Bytes;
use std::slice::Iter;

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(Bytes),
    Get(Bytes),
    Set([Bytes; 2]),
}

#[derive(Debug)]
pub enum Error {
    NotAnArray,
    MissingArgument,
    WrongType,
    UnknownCommand,
}

impl TryFrom<Frame> for Command {
    type Error = Error;
    fn try_from(value: Frame) -> Result<Self, Error> {
        let Frame::Array(Some(arr)) = value else {
            return Err(Error::NotAnArray);
        };
        let mut args = arr.iter();

        let command: Bytes = next_bytes(&mut args)?;

        return match arr.len() {
            1 if command.eq_ignore_ascii_case(b"ping") => Ok(Command::Ping),
            2 if command.eq_ignore_ascii_case(b"echo") => Ok(Command::Echo(next_bytes(&mut args)?)),
            2 if command.eq_ignore_ascii_case(b"get") => Ok(Command::Get(next_bytes(&mut args)?)),
            3 if command.eq_ignore_ascii_case(b"set") => Ok(Command::Set([
                next_bytes(&mut args)?,
                next_bytes(&mut args)?,
            ])),
            _ => Err(Error::UnknownCommand),
        };
    }
}

/// Advances the iterator and returns the next value.
///
/// Returns `Err(Error::MissingArgument)` if the next item is unavailable.
fn next<'a>(it: &'a mut Iter<'_, Frame>) -> Result<&'a Frame, Error> {
    it.next().ok_or(Error::MissingArgument)
}

/// Advances the iterator and returns the `Bytes` contained in next value.
///
/// Returns:
/// - `Err(Error::MissingArgument)` if the next item is unavailable
/// - `Err(Error::WrongType)` if the next item does not contain `Bytes`
fn next_bytes<'a>(it: &mut Iter<'_, Frame>) -> Result<Bytes, Error> {
    next(it)?.get_bytes().ok_or(Error::WrongType)
}

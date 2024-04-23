use crate::frame::Frame;
use bytes::BytesMut;
use tokio::time::{Duration, Instant};

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo {
        message: BytesMut,
    },
    Get {
        key: BytesMut,
    },
    Set {
        key: BytesMut,
        value: BytesMut,
        // must_overwrite: Option<bool>,
        expires: Option<Instant>,
    },
}

#[derive(Debug, PartialEq)]
pub enum Error {
    NotAnArray,
    MissingArgument,
    UnexpectedArgument,
    WrongType,
    UnknownCommand,
}

impl TryFrom<Frame> for Command {
    type Error = Error;
    fn try_from(value: Frame) -> Result<Self, Error> {
        let Frame::Array(Some(arr)) = value else {
            return Err(Error::NotAnArray);
        };

        let mut args = arr.into_iter();

        macro_rules! next {
            ($f:ident) => {{
                args.next()
                    .ok_or(Error::MissingArgument)?
                    .$f()
                    .ok_or(Error::WrongType)?
            }};
        }

        macro_rules! next_uint {
            () => {
                TryInto::<u64>::try_into(next!(i64)).or(Err(Error::WrongType))?
            };
        }

        macro_rules! optional_args {
            () => {{}};
        }

        // optional_args!(
        //     b"ex"| b"px" => expires = Instant::now() + Duration::from_secs(next_uint!())
        // )
        optional_args!(
            expires = {
                b"ex" => Instant::now() + Duration::from_secs(next_uint!())
                b"px" => Instant::now() + Duration::from_millis(next_uint!())
            }
        )

        let mut command = next!(bytes);

        command.make_ascii_lowercase();

        return match &command[..] {
            b"ping" => Ok(Command::Ping),
            b"echo" => Ok(Command::Echo {
                message: next!(bytes),
            }),
            b"get" => Ok(Command::Get { key: next!(bytes) }),
            b"set" => {
                let key = next!(bytes);
                let value = next!(bytes);
                let mut expires = None;

                while let Some(arg) = args.next() {
                    let mut arg = arg.bytes().ok_or(Error::WrongType)?;
                    arg.make_ascii_lowercase();
                    let arg = &arg[..];
                    match arg {
                        b"ex" | b"px" => {
                            expires = Some(
                                Instant::now()
                                    + match n {
                                        b"ex" => Duration::from_secs(next_uint!()),
                                        b"px" => Duration::from_millis(next_uint!()),
                                    },
                            )
                        }
                        _ => return Err(Error::UnexpectedArgument),
                    };
                }

                Ok(Command::Set {
                    key,
                    value,
                    expires,
                })
            }
            _ => Err(Error::UnknownCommand),
        };
    }
}

impl Command {
    /// Returns whether the command modifies the db
    pub fn is_write(&self) -> bool {
        match self {
            Command::Ping | Command::Echo { .. } | Command::Get { .. } => false,
            Command::Set { .. } => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping() {
        assert_eq!(
            Frame::Array(Some(vec![Frame::String("pIng".into())])).try_into(),
            Ok(Command::Ping)
        )
    }

    #[test]
    fn echo() {
        assert_eq!(
            Frame::Array(Some(vec![
                Frame::String("echo".into()),
                Frame::String("hello".into())
            ]))
            .try_into(),
            Ok(Command::Echo {
                message: "hello".into()
            })
        )
    }

    #[test]
    fn get() {
        assert_eq!(
            Frame::Array(Some(vec![
                Frame::String("get".into()),
                Frame::String("key".into())
            ]))
            .try_into(),
            Ok(Command::Get { key: "key".into() })
        )
    }

    #[test]
    fn set() {
        assert_eq!(
            Frame::Array(Some(vec![
                Frame::String("set".into()),
                Frame::String("key".into()),
                Frame::String("value".into())
            ]))
            .try_into(),
            Ok(Command::Set {
                key: "key".into(),
                value: "value".into(),
                expires: None
            })
        )
    }

    #[tokio::test(start_paused = true)]
    async fn set_ex() {
        assert_eq!(
            Frame::Array(Some(vec![
                Frame::String("set".into()),
                Frame::String("key".into()),
                Frame::String("value".into()),
                Frame::String("ex".into()),
                Frame::Integer(1)
            ]))
            .try_into(),
            Ok(Command::Set {
                key: "key".into(),
                value: "value".into(),
                expires: Some(Instant::now() + Duration::from_secs(1))
            })
        )
    }

    #[test]
    fn not_an_array() {
        assert_eq!(
            Frame::String("not an array".into()).try_into(),
            Err::<Command, Error>(Error::NotAnArray)
        )
    }

    #[test]
    fn missing_argument() {
        let t: Result<Command, Error> =
            Frame::Array(Some(vec![Frame::String("echo".into())])).try_into();
        println!("{:?}", t);
        assert_eq!(
            Frame::Array(Some(vec![Frame::String("echo".into())])).try_into(),
            Err::<Command, Error>(Error::MissingArgument)
        )
    }

    // #[test]
    // fn wrong_type() {
    //     assert_eq!(
    //         Frame::Array(Some(vec![
    //             Frame::String("echo".into()),
    //             Frame::Number(42.0)
    //         ]))
    //         .try_into::<Command>(),
    //         Err(Error::WrongType)
    //     )
    // }

    #[test]
    fn unknown_command() {
        assert_eq!(
            Frame::Array(Some(vec![
                Frame::String("unknown".into()),
                Frame::String("arg".into())
            ]))
            .try_into(),
            Err::<Command, Error>(Error::UnknownCommand)
        )
    }
}

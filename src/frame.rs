use bytes::BytesMut;

macro_rules! build_matching_prefix_and_frame_enums {
    ($($name:ident$(($type:ty))? = $value:literal$(,)?)*) => {

        pub enum Prefix {
            $(
                $name,
            )*
        }

        pub struct InvalidPrefix;

        impl TryFrom<u8> for Prefix {
            type Error = InvalidPrefix;
            fn try_from(value: u8) -> Result<Self, InvalidPrefix> {
                match value {
                    $(
                        $value => Ok(Prefix::$name),
                    )*
                    _ => Err(InvalidPrefix),
                }
            }
        }

        #[derive(Debug, PartialEq)]
        #[repr(u8)]
        pub enum Frame {
            $(
                $name $(($type))? = $value,
            )*
        }
    };
}

// https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
build_matching_prefix_and_frame_enums! {
    Array(Option<Vec<Frame>>) = b'*',
    Boolean(bool) = b'#',
    Bulk(Option<BytesMut>) = b'$',
    Error(BytesMut) = b'-',
    Integer(i64) = b':',
    Null = b'_',
    String(BytesMut) = b'+',
}

impl Frame {
    pub fn bytes(self) -> Option<BytesMut> {
        Some(match self {
            Frame::String(buf) => buf,
            Frame::Bulk(Some(buf)) => buf,
            _ => return None,
        })
    }

    pub fn i64(self) -> Option<i64> {
        Some(match self {
            Frame::Integer(i) => i,
            _ => return None,
        })
    }

    pub fn prefix(&self) -> u8 {
        // SAFETY: Because `Self` is marked `repr(u8)`, its layout is a `repr(C)` `union`
        // between `repr(C)` structs, each of which has the `u8` discriminant as its first
        // field, so we can read the discriminant without offsetting the pointer.
        unsafe { *<*const _>::from(self).cast::<u8>() }
    }
}

pub struct Bool(bool);
pub struct InvalidBool;

impl TryFrom<&[u8]> for Bool {
    type Error = InvalidBool;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"t" => Ok(Self(true)),
            b"f" => Ok(Self(false)),
            _ => Err(InvalidBool),
        }
    }
}

impl From<Bool> for bool {
    fn from(value: Bool) -> Self {
        let Bool(b) = value;
        b
    }
}

impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        Bool(value)
    }
}

impl From<Bool> for u8 {
    fn from(value: Bool) -> Self {
        let Bool(b) = value;
        match b {
            true => b't',
            false => b'f',
        }
    }
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::{Bytes, BytesMut};

use crate::{command::Command, frame::Frame};

pub struct Db {
    state: Arc<Mutex<State>>,
}

struct State {
    // TODO: consider the implications of sharing a BytesMut across threads
    // TODO: do we need a BytesMut for any reason other than satisfying the Frame interface?
    keystore: HashMap<Bytes, BytesMut>,
}

impl Db {
    /// Creates a new database
    pub fn new() -> Self {
        Db {
            state: Arc::new(Mutex::new(State {
                keystore: HashMap::new(),
            })),
        }
    }
}

impl Db {
    pub fn apply(&self, command: Command) -> Frame {
        match command {
            Command::Ping => Frame::Bulk(Some("PONG".into())),
            Command::Echo { message } => Frame::Bulk(Some(message)),
            Command::Set { key, value, .. } => {
                let _ = self
                    .state
                    .lock()
                    .unwrap()
                    .keystore
                    .insert(key.into(), value.into());
                Frame::Bulk(Some("OK".into()))
            }
            Command::Get { key } => Frame::Bulk(
                self.state
                    .lock()
                    .unwrap()
                    .keystore
                    .get(&key.freeze())
                    .map(|b| b.clone()),
            ),
        }
    }
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Db {
            state: self.state.clone(),
        }
    }
}

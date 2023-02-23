use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    num::ParseIntError,
    str::FromStr,
    sync::atomic::{AtomicI32, Ordering},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DeserializeFromStr, SerializeDisplay};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct MsgId(i32);

impl MsgId {
    pub fn next() -> Self {
        static COUNTER: AtomicI32 = AtomicI32::new(0);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Hash, SerializeDisplay, DeserializeFromStr,
)]
pub enum Id {
    Node(i32),
    Client(i32),
}

impl Id {
    pub fn id(&self) -> i32 {
        match self {
            Id::Node(i) => *i,
            Id::Client(i) => *i,
        }
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Id::Node(i) => write!(f, "n{}", i),
            Id::Client(i) => write!(f, "c{}", i),
        }
    }
}

impl FromStr for Id {
    type Err = IdParseErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix('n') {
            return rest.parse().map_err(IdParseErr::BadInt).map(Self::Node);
        }

        if let Some(rest) = s.strip_prefix('c') {
            return rest.parse().map_err(IdParseErr::BadInt).map(Self::Client);
        }

        Err(IdParseErr::MissingPrefix)
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum IdParseErr {
    #[error("node id did not start with prefix")]
    MissingPrefix,

    #[error("could not parse int: {0}")]
    BadInt(ParseIntError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message<T> {
    pub src: Id,
    pub dest: Id,
    pub body: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub src: Id,
    pub dest: Id,
    pub body: ResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RequestBody {
    Read {
        msg_id: MsgId,
    },
    Echo {
        echo: Value,
        msg_id: MsgId,
    },
    Init {
        msg_id: MsgId,
        node_id: Id,
        node_ids: Vec<Id>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResponseBody {
    ReadOk {
        msg_id: Option<MsgId>,
        in_reply_to: MsgId,
    },
    EchoOk {
        echo: Value,
        msg_id: Option<MsgId>,
        in_reply_to: MsgId,
    },
}

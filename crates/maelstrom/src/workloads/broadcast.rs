use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::messages::Id;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    pub topology: HashMap<Id, HashSet<Id>>,
    pub msg_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyOk {
    msg_id: Option<i32>,
    in_reply_to: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Broadcast<T> {
    pub msg_id: i32,
    pub message: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BroadcastOk {
    msg_id: Option<i32>,
    in_reply_to: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Read {
    pub msg_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadOk<T> {
    messages: Vec<T>,
    msg_id: Option<i32>,
    in_reply_to: i32,
}

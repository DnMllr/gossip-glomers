use std::collections::{HashMap, HashSet};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    client::{Client, Result},
    messages::{Id, Message, MsgId},
};

pub fn from_client(client: Client) -> Workload {
    Workload::new(client)
}

pub async fn new() -> Result<Workload> {
    Client::connect().await.map(from_client)
}

#[derive(Debug)]
pub struct Workload {
    client: Client,
}

impl Workload {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn nodes(&self) -> &[Id] {
        self.client.nodes()
    }

    pub fn node_id(&self) -> Id {
        self.client.id()
    }

    pub async fn recv<T>(&self) -> Result<Option<Request<T>>>
    where
        T: DeserializeOwned + std::fmt::Debug,
    {
        self.client
            .recv()
            .await
            .map(|maybe_msg| maybe_msg.map(|msg| self.handle_msg(msg)))
    }

    fn handle_msg<T>(&self, msg: Message<RequestBody<T>>) -> Request<T> {
        match msg.body {
            RequestBody::Topology { topology, msg_id } => Request::Topology(TopologyRequest {
                client: self.client.clone(),
                from: msg.src,
                topology,
                msg_id,
            }),
            RequestBody::Broadcast { message, msg_id } => Request::Broadcast(BroadcastRequest {
                client: self.client.clone(),
                from: msg.src,
                msg_id,
                message,
            }),
            RequestBody::Read { msg_id } => Request::Read(ReadRequest {
                client: self.client.clone(),
                from: msg.src,
                msg_id,
            }),
            RequestBody::BroadcastOk { in_reply_to, .. } => Request::Ack(in_reply_to),
        }
    }

    pub async fn send<T>(&self, to: Id, message: T) -> Result<MsgId>
    where
        T: Serialize + std::fmt::Debug,
    {
        let id = MsgId::next();

        self.client
            .send(
                to,
                RequestBody::Broadcast {
                    message,
                    msg_id: id,
                },
            )
            .await?;

        Ok(id)
    }

    pub async fn retry<T>(&self, msg_id: MsgId, to: Id, message: T) -> Result<()>
    where
        T: Serialize + std::fmt::Debug,
    {
        self.client
            .send(to, RequestBody::Broadcast { message, msg_id })
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum Request<T> {
    Read(ReadRequest),
    Broadcast(BroadcastRequest<T>),
    Topology(TopologyRequest),
    Ack(MsgId),
}

#[derive(Debug)]
pub struct ReadRequest {
    from: Id,
    msg_id: MsgId,
    client: Client,
}

impl ReadRequest {
    pub async fn reply<T>(&self, messages: &[T]) -> Result<()>
    where
        T: Serialize + std::fmt::Debug,
    {
        self.client
            .send(
                self.from,
                ResponseBody::ReadOk {
                    msg_id: MsgId::next(),
                    in_reply_to: self.msg_id,
                    messages,
                },
            )
            .await
    }
}

#[derive(Debug)]
pub struct BroadcastRequest<T> {
    message: T,
    from: Id,
    msg_id: MsgId,
    client: Client,
}

impl<T> BroadcastRequest<T>
where
    T: Serialize,
{
    pub fn message(&self) -> &T {
        &self.message
    }

    pub fn from(&self) -> &Id {
        &self.from
    }

    pub async fn reply(&self) -> Result<()> {
        self.client
            .send(
                self.from,
                ResponseBody::BroadcastOk::<()> {
                    msg_id: MsgId::next(),
                    in_reply_to: self.msg_id,
                },
            )
            .await
    }
}

#[derive(Debug)]
pub struct TopologyRequest {
    from: Id,
    topology: HashMap<Id, HashSet<Id>>,
    msg_id: MsgId,
    client: Client,
}

impl TopologyRequest {
    pub fn topology(&self) -> &HashMap<Id, HashSet<Id>> {
        &self.topology
    }

    pub fn neighbors(&self) -> Option<&HashSet<Id>> {
        self.topology.get(&self.client.id())
    }

    pub async fn reply(&self) -> Result<()> {
        self.client
            .send(
                self.from,
                ResponseBody::TopologyOk::<()> {
                    msg_id: MsgId::next(),
                    in_reply_to: self.msg_id,
                },
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestBody<T> {
    Topology {
        topology: HashMap<Id, HashSet<Id>>,
        msg_id: MsgId,
    },
    Broadcast {
        message: T,
        msg_id: MsgId,
    },
    Read {
        msg_id: MsgId,
    },
    // Other nodes could be ack-ing our gossip
    BroadcastOk {
        msg_id: MsgId,
        in_reply_to: MsgId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody<'a, T> {
    TopologyOk {
        msg_id: MsgId,
        in_reply_to: MsgId,
    },
    BroadcastOk {
        msg_id: MsgId,
        in_reply_to: MsgId,
    },
    ReadOk {
        messages: &'a [T],
        msg_id: MsgId,
        in_reply_to: MsgId,
    },
}

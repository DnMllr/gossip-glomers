use serde::{Deserialize, Serialize};

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

    pub fn node_id(&self) -> i32 {
        self.client.id().id()
    }

    pub async fn recv(&self) -> Result<Option<Request>> {
        self.client
            .recv()
            .await
            .map(|maybe_msg| maybe_msg.map(|msg| self.handle_msg(msg)))
    }

    fn handle_msg(&self, msg: Message<RequestBody>) -> Request {
        let RequestBody::Generate { msg_id } = msg.body;
        Request {
            from: msg.src,
            in_reply_to: msg_id,
            client: self.client.clone(),
        }
    }
}

pub struct Request {
    from: Id,
    in_reply_to: MsgId,
    client: Client,
}

impl Request {
    pub async fn generate<T>(self, id: T) -> Result<()>
    where
        T: std::fmt::Debug + Serialize,
    {
        self.client
            .send(
                self.from,
                ResponseBody::GenerateOk {
                    msg_id: MsgId::next(),
                    in_reply_to: self.in_reply_to,
                    id,
                },
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestBody {
    Generate { msg_id: MsgId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody<T> {
    GenerateOk {
        id: T,
        msg_id: MsgId,
        in_reply_to: MsgId,
    },
}

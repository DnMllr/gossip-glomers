use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    client::{Client, Error},
    messages::{Id, Message, MsgId},
};

pub fn from_client<T>(client: Client) -> Workload<T> {
    Workload::new(client)
}

pub async fn new<T>() -> Result<Workload<T>, Error> {
    Client::connect().await.map(from_client)
}

#[derive(Debug)]
pub struct Workload<T> {
    client: Client,
    payload_type: PhantomData<T>,
}

impl<T> Workload<T> {
    pub fn new(client: Client) -> Self {
        Self {
            payload_type: PhantomData,
            client,
        }
    }
}

impl<T> Workload<T>
where
    T: std::fmt::Debug + Serialize + DeserializeOwned,
{
    pub async fn recv(&self) -> Result<Option<Request<T>>, Error> {
        let request = self
            .client
            .recv()
            .await?
            .and_then(|msg| self.handle_msg(msg));

        Ok(request)
    }

    fn handle_msg(&self, msg: Message<Body<T>>) -> Option<Request<T>> {
        match msg.body {
            Body::Echo(echo) => Some(Request {
                from: msg.src,
                data: echo,
                client: self.client.clone(),
            }),
            Body::EchoOk(_) => None,
        }
    }
}

pub struct Request<T> {
    pub from: Id,
    pub data: Echo<T>,
    client: Client,
}

impl<'de, T> Request<T>
where
    T: std::fmt::Debug + Serialize + Deserialize<'de>,
{
    pub async fn echo(self) -> Result<(), Error> {
        self.client
            .send(
                self.from,
                Body::EchoOk(EchoOk {
                    echo: self.data.echo,
                    msg_id: MsgId::next(),
                    in_reply_to: self.data.msg_id,
                }),
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Body<T> {
    Echo(Echo<T>),
    EchoOk(EchoOk<T>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Echo<T> {
    pub echo: T,
    pub msg_id: MsgId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EchoOk<T> {
    echo: T,
    msg_id: MsgId,
    in_reply_to: MsgId,
}

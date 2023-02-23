use std::sync::Arc;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdin, Stdout},
    sync::Mutex,
};

use crate::messages::{Id, Message, MsgId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Services>,
    nodes: Arc<Vec<Id>>,
    id: Id,
}

impl Client {
    pub fn id(&self) -> Id {
        self.id
    }

    #[tracing::instrument(err)]
    pub async fn connect() -> Result<Self> {
        tracing::info!("connecting");
        let mut buffer = String::with_capacity(64);
        let services = Services::new();
        services.read_line(&mut buffer).await?;

        let init: Message<Init> = serde_json::from_str(&buffer)?;

        tracing::info!(msg = ?init, "saw msg");

        let from = init.src;
        let msg_id = init.body.msg_id;

        let client = Self::from_init(init, services);

        client
            .send(
                from,
                ResponseBody::InitOk {
                    in_reply_to: msg_id,
                },
            )
            .await?;

        Ok(client)
    }

    fn from_init(msg: Message<Init>, services: Services) -> Self {
        Self {
            id: msg.body.node_id,
            nodes: Arc::new(msg.body.node_ids),
            inner: Arc::new(services),
        }
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn recv<T>(&self) -> Result<Option<T>>
    where
        T: std::fmt::Debug + DeserializeOwned,
    {
        let mut buf = String::with_capacity(64);
        let amount_read = self.inner.read_line(&mut buf).await?;
        Ok(if amount_read == 0 {
            None
        } else {
            let value = serde_json::from_str(&buf)?;
            tracing::info!(msg = ?value, "received msg");
            Some(value)
        })
    }

    #[tracing::instrument(skip(self), err)]
    pub async fn send<T: Serialize + std::fmt::Debug>(&self, to: Id, body: T) -> Result<()> {
        let msg = Message {
            src: self.id,
            dest: to,
            body,
        };

        tracing::info!(?msg, "sending msg");

        let data = serde_json::to_string(&msg)?;

        self.inner.write_line(&data).await?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Init {
    msg_id: MsgId,
    node_id: Id,
    node_ids: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody {
    InitOk { in_reply_to: MsgId },
}

#[derive(Debug)]
struct Services {
    stdout: Mutex<BufWriter<Stdout>>,
    stdin: Mutex<BufReader<Stdin>>,
}

impl Services {
    fn new() -> Self {
        Self {
            stdout: Mutex::new(BufWriter::new(tokio::io::stdout())),
            stdin: Mutex::new(BufReader::new(tokio::io::stdin())),
        }
    }

    async fn read_line(&self, buffer: &mut String) -> std::io::Result<usize> {
        self.stdin.lock().await.read_line(buffer).await
    }

    async fn write_line(&self, buffer: &str) -> std::io::Result<()> {
        let mut out = self.stdout.lock().await;
        out.write_all(buffer.as_bytes()).await?;
        out.write_all(b"\n").await?;
        out.flush().await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

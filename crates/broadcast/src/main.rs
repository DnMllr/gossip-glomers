//! On this page [https://fly.io/dist-sys/3d/] it says to ignore the topology, so i'm doing that.
use std::{collections::HashSet, sync::Arc, time::Duration};

use dashmap::{mapref::one::RefMut, DashMap, DashSet};
use maelstrom::{
    client,
    messages::{Id, MsgId},
    workloads::broadcast::{self, BroadcastRequest, Request, Workload},
};
use tokio::{sync::mpsc, time::Instant};
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> client::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .pretty()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let (send, mut recv) = mpsc::channel(1);
    let workload = broadcast::new().await?;
    let state = Arc::new(State::new());

    let sync = state.clone();
    let w = workload.clone();
    tokio::spawn(async move { sync.spawn_sync(w).await });

    while let Some(req) = workload.recv::<i32>().await? {
        let s = state.clone();
        let w = workload.clone();
        let send = send.clone();
        tokio::spawn(async move { s.handle_request(req, w, send).await });
    }

    drop(send);

    let _ = recv.recv().await;

    Ok(())
}

struct State {
    remotes: DashMap<Id, HashSet<i32, fxhash::FxBuildHasher>, fxhash::FxBuildHasher>,
    messages: DashSet<i32, fxhash::FxBuildHasher>,
    pending: PendingMessages,
}

impl State {
    fn new() -> Self {
        Self {
            messages: DashSet::with_hasher(fxhash::FxBuildHasher::default()),
            remotes: DashMap::with_hasher(fxhash::FxBuildHasher::default()),
            pending: PendingMessages::new(),
        }
    }

    #[tracing::instrument(skip(self, req, workload, _channel), fields(n = %workload.node_id()), err)]
    async fn handle_request(
        &self,
        req: Request<i32>,
        workload: Workload,
        _channel: mpsc::Sender<()>,
    ) -> client::Result<()> {
        match req {
            Request::BroadcastOk(msg_id) => {
                self.ack_msg(msg_id);
            }

            Request::BroadcastAllOk(msg_id, id, messages) => {
                self.ack_msg(msg_id);
                for msg in messages {
                    if self.messages.insert(msg) {
                        self.append_if_already_buffered(&id, msg, &workload);
                    }
                }
            }

            Request::Read(read) => {
                let mut v = self.messages.iter().map(|v| *v).collect::<Vec<i32>>();
                v.sort();
                read.reply(v.as_slice()).await?;
            }

            Request::Broadcast(b) => {
                if self.messages.insert(*b.message()) {
                    tracing::info!(msg = *b.message(), "saw new message");
                    self.new_msg(b.from(), *b.message(), &workload);
                }

                b.reply().await?;
            }

            Request::Topology(topology) => {
                topology.reply().await?;
            }

            Request::BroadcastAll(sync) => {
                tracing::info!(msgs = ?sync.messages(), "saw broadcast all with messages");
                for msg in sync.messages() {
                    if self.messages.insert(*msg) {
                        self.append_if_already_buffered(sync.from(), *msg, &workload);
                    }
                }
                sync.reply(&[]).await?;
            }
        }

        Ok(())
    }

    fn new_msg(&self, from: &Id, msg: i32, workload: &Workload) {
        for node in workload.nodes() {
            if node != &workload.node_id() && node != from {
                self.remote_mut(*node).insert(msg);
            }
        }
    }

    fn append_if_already_buffered(&self, from: &Id, msg: i32, workload: &Workload) {
        for node in workload.nodes() {
            if node != &workload.node_id() && node != from {
                let mut remote = self.remote_mut(*node);
                if !remote.is_empty() {
                    remote.insert(msg);
                }
            }
        }
    }

    fn remote_mut(
        &self,
        id: Id,
    ) -> RefMut<Id, HashSet<i32, fxhash::FxBuildHasher>, fxhash::FxBuildHasher> {
        self.remotes
            .entry(id)
            .or_insert_with(fxhash::FxHashSet::default)
    }

    async fn spawn_sync(&self, workload: Workload) -> client::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.sync(&workload).await?;
        }
    }

    #[tracing::instrument(skip(self, workload), err)]
    async fn sync(&self, workload: &Workload) -> client::Result<()> {
        let mut nodes = workload.nodes().to_owned();
        nodes.sort();
        for node in nodes {
            if node != workload.node_id() {
                self.sync_node(&node, workload).await?;
            }
        }

        Ok(())
    }

    async fn sync_node(&self, node: &Id, workload: &Workload) -> client::Result<()> {
        let set: HashSet<i32, fxhash::FxBuildHasher> =
            self.remote_mut(*node).iter().cloned().collect();

        if !set.is_empty() {
            tracing::info!(diff = ?set, target = %node, "syncing diff");
            let mut list: Vec<i32> = set.iter().cloned().collect();
            list.sort();
            let msg_id = workload.send_all(*node, list).await?;

            self.pending.set(
                msg_id,
                PendingMessage {
                    to: *node,
                    messages: set,
                    time: Instant::now(),
                },
            );
        }

        Ok(())
    }

    fn ack_msg(&self, msg_id: MsgId) {
        if let Some(p) = self.pending.ack(msg_id) {
            for msg in p.messages {
                self.remote_mut(p.to).remove(&msg);
            }
        }
    }
}

#[derive(Default)]
pub struct PendingMessages {
    messages: DashMap<MsgId, PendingMessage>,
}

pub struct PendingMessage {
    pub to: Id,
    pub messages: HashSet<i32, fxhash::FxBuildHasher>,
    pub time: Instant,
}

impl PendingMessages {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ack(&self, msg_id: MsgId) -> Option<PendingMessage> {
        self.messages.remove(&msg_id).map(|(_, p)| p)
    }

    pub fn set(&self, msg_id: MsgId, pending: PendingMessage) {
        self.messages.insert(msg_id, pending);
    }
}

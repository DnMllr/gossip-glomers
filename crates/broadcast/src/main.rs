//! On this page [https://fly.io/dist-sys/3d/] it says to ignore the topology, so i'm doing that.
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use dashmap::{mapref::one::RefMut, DashMap, DashSet};
use futures::{stream::FuturesUnordered, TryStreamExt};
use maelstrom::{
    client,
    messages::{Id, MsgId},
    workloads::broadcast::{
        self, BroadcastRequest, ReadRequest, Request, Sync, TopologyRequest, Workload,
    },
};
use tokio::{sync::mpsc, time::Instant};
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// As per https://fly.io/dist-sys/3e/ the goal is the following:
//
// 1. Messages-per-operation is below 20
// 2. Median latency is below 1 second
// 3. Maximum latency is below 2 seconds.
//
// This solution results in the following:
// :servers {:send-count 3406,
//           :recv-count 3406,
//           :msg-count 3406,
//           :msgs-per-op 1.9871645},
// :stable-latencies {0 0,
//                    0.5 339,
//                    0.95 596,
//                    0.99 636,
//                    1 678},
//
// Which meets requirements. It's also resiliant to `nemesis --partition` (though that resiliancy could be improved by moving the broadcaster role to a
// non partitioned node if the broadcaster has gotten partitioned).
//

const BROADCASTER_HEARTBEAT: Duration = Duration::from_millis(500);
const FORWARDER_HEARTBEAT: Duration = Duration::from_millis(100);
const MAX_MSGS: usize = 100;
const RETRY: Duration = Duration::from_millis(1000);

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

#[derive(Debug, Default)]
struct State {
    remotes: Remotes,
    messages: Messages,
    pending: PendingMessages,
}

impl State {
    fn new() -> Self {
        Default::default()
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
                self.on_broadcast_all_ack(id, messages, &workload).await?;
            }

            Request::Read(read) => {
                self.on_read(read).await?;
            }

            Request::Broadcast(b) => {
                self.on_broadcast(b, &workload).await?;
            }

            Request::Topology(topology) => {
                self.on_topology(topology).await?;
            }

            Request::BroadcastAll(sync) => {
                self.on_sync(sync, &workload).await?;
            }
        }

        Ok(())
    }

    async fn on_broadcast(
        &self,
        b: BroadcastRequest<i32>,
        workload: &Workload,
    ) -> client::Result<()> {
        if self.messages.add(*b.message()) {
            tracing::info!(msg = *b.message(), "saw new message");
            tokio::try_join!(b.reply(), self.new_msg(b.from(), *b.message(), workload))?;
        } else {
            b.reply().await?;
        }

        Ok(())
    }

    async fn on_broadcast_all_ack(
        &self,
        from: Id,
        messages: Vec<i32>,
        workload: &Workload,
    ) -> client::Result<()> {
        self.new_messages(&from, messages.into_iter(), workload)
            .await
    }

    async fn on_sync(&self, sync: Sync<i32>, workload: &Workload) -> client::Result<()> {
        tracing::info!(msgs = ?sync.messages(), "saw broadcast all with messages");

        let msgs = self.remotes.get_mut(*sync.from()).drain_to_vec();

        let reply_fut = async {
            let msg_id = sync.reply(&msgs).await?;

            self.pending.set(
                msg_id,
                PendingMessage::new(*sync.from(), msgs.into_iter().collect()),
            );

            client::Result::Ok(())
        };

        let send_fut = self.new_messages(sync.from(), sync.messages().iter().copied(), workload);

        tokio::try_join!(reply_fut, send_fut)?;

        Ok(())
    }

    async fn new_messages<I: Iterator<Item = i32>>(
        &self,
        from: &Id,
        messages: I,
        workload: &Workload,
    ) -> client::Result<()> {
        let mut futures = FuturesUnordered::new();
        for msg in messages {
            if self.messages.add(msg) {
                futures.push(self.new_msg(from, msg, workload));
            }
        }

        while (futures.try_next().await?).is_some() {}

        Ok(())
    }

    async fn on_read(&self, read: ReadRequest) -> client::Result<()> {
        self.messages.send(read).await
    }

    async fn on_topology(&self, topology: TopologyRequest) -> client::Result<()> {
        topology.reply().await
    }

    async fn new_msg(&self, from: &Id, msg: i32, workload: &Workload) -> client::Result<()> {
        if node_is_broadcaster(from, workload) {
            return Ok(());
        }

        if i_am_broadcaster(workload) {
            self.broadcast(from, msg, workload).await
        } else {
            self.forward_to_broadcaster(msg, workload).await
        }
    }

    async fn broadcast(&self, from: &Id, msg: i32, workload: &Workload) -> client::Result<()> {
        let nodes = workload.nodes();
        let mut futures = FuturesUnordered::new();
        for node in nodes {
            if node != &workload.node_id() && node != from {
                let mut remote = self.remotes.get_mut(*node);
                remote.add(msg);
                if remote.len() > MAX_MSGS {
                    futures.push(self.sync_node(*node, workload));
                }
            }
        }

        while (futures.try_next().await?).is_some() {}

        Ok(())
    }

    async fn forward_to_broadcaster(&self, msg: i32, workload: &Workload) -> client::Result<()> {
        let broadcaster = &broadcaster(workload);
        let mut remote = self.remotes.get_mut(*broadcaster);
        remote.add(msg);
        if remote.len() > MAX_MSGS {
            self.sync_node(*broadcaster, workload).await?;
        }

        Ok(())
    }

    async fn spawn_sync(&self, workload: Workload) -> client::Result<()> {
        loop {
            if i_am_broadcaster(&workload) {
                tokio::time::sleep(BROADCASTER_HEARTBEAT).await;
            } else {
                tokio::time::sleep(FORWARDER_HEARTBEAT).await;
            }
            self.sync(&workload).await?;
            self.retry(&workload).await?;
        }
    }

    #[tracing::instrument(skip(self, workload), err)]
    async fn sync(&self, workload: &Workload) -> client::Result<()> {
        let nodes = workload.nodes();
        let mut futures = FuturesUnordered::new();
        for node in nodes {
            if node != &workload.node_id() {
                futures.push(self.sync_node(*node, workload));
            }
        }

        while (futures.try_next().await?).is_some() {}

        Ok(())
    }

    #[tracing::instrument(skip(self, workload), err)]
    async fn retry(&self, workload: &Workload) -> client::Result<()> {
        let mut futures = FuturesUnordered::new();

        for (node, set) in self.pending.retry() {
            let mut msgs: Vec<i32> = set.iter().copied().collect();
            msgs.sort();
            futures.push(async move {
                let msg_id = workload.send_all(node, msgs).await?;
                self.pending.set(msg_id, PendingMessage::new(node, set));
                client::Result::Ok(())
            });
        }

        while (futures.try_next().await?).is_some() {}

        Ok(())
    }

    async fn sync_node(&self, node: Id, workload: &Workload) -> client::Result<()> {
        let set = self.remotes.get_mut(node).take();

        if !set.is_empty() {
            tracing::info!(diff = ?set, target = %node, "syncing diff");
            let mut list: Vec<i32> = set.iter().copied().collect();
            list.sort();
            let msg_id = workload.send_all(node, list).await?;

            self.pending.set(msg_id, PendingMessage::new(node, set));
        }

        Ok(())
    }

    fn ack_msg(&self, msg_id: MsgId) {
        if let Some(p) = self.pending.ack(msg_id) {
            for msg in p.messages {
                self.remotes.get_mut(p.to).remove(&msg);
            }
        }
    }
}

#[derive(Debug, Default)]
struct Messages {
    data: DashSet<i32, fxhash::FxBuildHasher>,
}

impl Messages {
    pub fn add(&self, msg: i32) -> bool {
        self.data.insert(msg)
    }

    async fn send(&self, reply: ReadRequest) -> client::Result<()> {
        let mut v = self.data.iter().map(|v| *v).collect::<Vec<i32>>();
        v.sort();
        reply.reply(v.as_slice()).await
    }
}

#[derive(Debug, Default)]
struct Remotes {
    remotes: DashMap<Id, Remote, fxhash::FxBuildHasher>,
}

impl Remotes {
    pub fn get_mut(&self, id: Id) -> RefMut<Id, Remote, fxhash::FxBuildHasher> {
        self.remotes.entry(id).or_default()
    }
}

#[derive(Debug, Default)]
struct Remote {
    data: HashSet<i32, fxhash::FxBuildHasher>,
}

impl Remote {
    pub fn add(&mut self, msg: i32) -> bool {
        self.data.insert(msg)
    }

    pub fn remove(&mut self, msg: &i32) -> bool {
        self.data.remove(msg)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn drain_to_vec(&mut self) -> Vec<i32> {
        let mut data: Vec<i32> = self.data.drain().collect();
        data.sort();
        data
    }

    pub fn take(&mut self) -> HashSet<i32, fxhash::FxBuildHasher> {
        std::mem::take(&mut self.data)
    }
}

#[derive(Debug, Default)]
pub struct PendingMessages {
    messages: DashMap<MsgId, PendingMessage>,
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

    pub fn retry(&self) -> HashMap<Id, HashSet<i32, fxhash::FxBuildHasher>, fxhash::FxBuildHasher> {
        let mut result =
            HashMap::<Id, HashSet<i32, fxhash::FxBuildHasher>, fxhash::FxBuildHasher>::default();

        let now = Instant::now();

        for mut r in self
            .messages
            .iter_mut()
            .filter(|m| now.duration_since(m.time) > RETRY && !m.retried)
        {
            result
                .entry(r.to)
                .or_default()
                .extend(r.messages.iter().copied());

            r.retried = true;
        }

        result
    }
}

#[derive(Debug)]
pub struct PendingMessage {
    pub to: Id,
    pub messages: HashSet<i32, fxhash::FxBuildHasher>,
    pub time: Instant,
    pub retried: bool,
}

impl PendingMessage {
    pub fn new(to: Id, messages: HashSet<i32, fxhash::FxBuildHasher>) -> Self {
        Self {
            to,
            messages,
            time: Instant::now(),
            retried: false,
        }
    }
}

fn i_am_broadcaster(workload: &Workload) -> bool {
    workload.node_id() == broadcaster(workload)
}

fn node_is_broadcaster(from: &Id, workload: &Workload) -> bool {
    from == &broadcaster(workload)
}

fn broadcaster(workload: &Workload) -> Id {
    workload
        .nodes()
        .iter()
        .min()
        .copied()
        .unwrap_or(workload.node_id())
}

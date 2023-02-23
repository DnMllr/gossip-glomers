use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

use base64::{engine::general_purpose, Engine};
use maelstrom::{
    client,
    workloads::unique_ids::{self, Request, Workload},
};
use rand::Rng;
use tokio::sync::mpsc::{channel, Sender};

#[tokio::main]
async fn main() -> client::Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let workload = unique_ids::new().await?;

    run_workload(workload).await
}

async fn run_workload(workload: Workload) -> client::Result<()> {
    let node_id = workload.node_id();

    let (tx, mut rx) = channel(1);

    while let Some(msg) = workload.recv().await? {
        tokio::spawn(generation_task(msg, node_id, tx.clone()));
    }

    drop(tx);

    let _ = rx.recv().await;

    Ok(())
}

async fn generation_task(msg: Request, node_id: i32, _send: Sender<()>) -> client::Result<()> {
    msg.generate(generate_id(node_id)).await?;
    Ok(())
}

#[tracing::instrument(skip_all)]
fn generate_id(node_id: i32) -> String {
    let mut id = [0; 32];
    let mut i = 0;

    let bytes = node_id
        .to_le_bytes()
        .into_iter()
        .chain(get_count().to_le_bytes().into_iter())
        .chain(random_number().to_le_bytes().into_iter());

    for byte in bytes {
        id[i] = byte;
        i += 1;
    }

    tracing::info!(bytes = ?&id[..i], "generated unique id");

    let mut s = String::with_capacity(i * 4 / 3 + 4);
    general_purpose::STANDARD.encode_string(&id[..i], &mut s);

    s
}

fn get_count() -> u32 {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn random_number() -> u32 {
    static RAND: AtomicU8 = AtomicU8::new(0);
    static VALUE: AtomicU32 = AtomicU32::new(0);
    if RAND.fetch_add(1, Ordering::Relaxed) == 0 {
        VALUE.store(rand::thread_rng().gen(), Ordering::Relaxed)
    }

    VALUE.load(Ordering::Relaxed)
}

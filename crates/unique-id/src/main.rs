use std::{
    sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering},
    time::SystemTime,
};

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

/// The size of an id's data
///
///  4 bytes of node id + 4 bytes counter + 4 random bytes + 8 byte timestamp
const ID_BUFFER_SIZE: usize = 18;

/// The size of a base64 encoded id
const ENCODED_BUFFER_SIZE: usize = ID_BUFFER_SIZE * 4 / 3;

type EncodedBuffer = [u8; ENCODED_BUFFER_SIZE];
const ENCODE_BUFF: EncodedBuffer = [0; ENCODED_BUFFER_SIZE];

async fn generation_task(msg: Request, node_id: i32, _send: Sender<()>) -> client::Result<()> {
    let mut buf = ENCODE_BUFF;
    generate_id(node_id, &mut buf);

    // I'm pretty sure it's ok to be unsafe here. We zeroed the memory and then base64 encoded the buffer.
    let id = unsafe { std::str::from_utf8_unchecked(&buf) };

    msg.generate(id).await?;
    Ok(())
}

fn generate_id(node_id: i32, buf: &mut EncodedBuffer) {
    let mut id = [0; 18];

    let bytes = node_id
        .to_le_bytes()
        .into_iter()
        .chain(get_count().to_le_bytes().into_iter())
        .chain(random_number().to_le_bytes().into_iter())
        .chain(timestamp().to_le_bytes().into_iter());

    for (id_byte, byte) in id.iter_mut().zip(bytes) {
        *id_byte = byte;
    }

    general_purpose::STANDARD
        .encode_slice(id, buf)
        .expect("size is constant, this should always succeed");
}

fn get_count() -> u32 {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn random_number() -> u16 {
    static SAMPLE: AtomicU8 = AtomicU8::new(0);
    static VALUE: AtomicU16 = AtomicU16::new(0);
    if SAMPLE.fetch_add(1, Ordering::Relaxed) == 0 {
        VALUE.store(rand::thread_rng().gen(), Ordering::Relaxed)
    }

    VALUE.load(Ordering::Relaxed)
}

fn timestamp() -> u64 {
    static SAMPLE: AtomicU8 = AtomicU8::new(0);
    static VALUE: AtomicU64 = AtomicU64::new(0);
    if SAMPLE.fetch_add(1, Ordering::Relaxed) == 0 {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock is definitey later than the unix epoch");
        VALUE.store(time.as_secs(), Ordering::Relaxed)
    }

    VALUE.load(Ordering::Relaxed)
}

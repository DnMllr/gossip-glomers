use maelstrom::{
    client,
    workloads::broadcast::{self, Request},
};

#[tokio::main]
async fn main() -> client::Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let workload = broadcast::new().await?;

    let mut msgs: Vec<i32> = Vec::with_capacity(128);

    while let Some(msg) = workload.recv().await? {
        match msg {
            Request::Read(read) => read.reply(&msgs).await?,
            Request::Broadcast(b) => {
                msgs.push(*b.message());
                b.reply().await?
            }
            Request::Topology(t) => t.reply().await?,
        }
    }

    Ok(())
}

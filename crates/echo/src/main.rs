use maelstrom::{client, workloads::echo};

#[tokio::main]
async fn main() -> client::Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_writer(std::io::stderr)
        .init();

    let workload = echo::new::<serde_json::Value>().await?;

    while let Some(msg) = workload.recv().await? {
        msg.echo().await?;
    }

    Ok(())
}

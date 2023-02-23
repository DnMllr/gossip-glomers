use maelstrom::{client, workloads::echo};

#[tokio::main]
async fn main() -> client::Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_writer(std::io::stderr)
        .init();

    let workload = echo::new().await?;

    run_workload(workload).await
}

async fn run_workload(workload: echo::Workload<serde_json::Value>) -> client::Result<()> {
    while let Some(msg) = workload.recv().await? {
        msg.echo().await?;
    }

    Ok(())
}

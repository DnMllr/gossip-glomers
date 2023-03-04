use client::{Client, Error};

pub mod client;
pub mod messages;
pub mod workloads;

pub async fn connect() -> Result<Client, Error> {
    Client::connect().await
}

#[async_trait::async_trait]
pub trait Request {
    type Response;

    async fn send(&self, client: &Client) -> client::Result<Self::Response>;
    async fn respond_with(&self, client: &Client, response: Self::Response) -> client::Result<()>;
}

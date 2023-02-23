use client::{Client, Error};

pub mod client;
pub mod messages;
pub mod workloads;

pub async fn connect() -> Result<Client, Error> {
    Client::connect().await
}

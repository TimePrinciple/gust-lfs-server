pub mod config;
pub mod content_store;
pub mod database;
pub mod server;

use anyhow::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    server::lfs_server().await.unwrap();

    Ok(())
}

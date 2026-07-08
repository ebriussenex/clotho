use std::time::Duration;

mod message;
mod node;
mod storage;
mod transport;

#[tokio::main]
async fn main() {
    //let _ = node::process(node::Config {})
    //    .await
    //    .inspect_err(|e| println!("processing failed: {e}"));
}

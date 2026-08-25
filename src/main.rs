use crate::{io::stdio::StdioReader, io::stdio::StdioWriter, node::Config};

use tokio::{self};

mod io;
mod maelstrom;
mod message;
mod node;
mod storage;
mod transport;

fn init_tracing() {
    use std::io::IsTerminal;

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init()
}

#[tokio::main]
async fn main() {
    init_tracing();

    let mut msg_reader = StdioReader::default();
    let mut msg_writer = StdioWriter::default();
    let config = Config::default();

    let (node_id, node_ids) = match maelstrom::handshake(&mut msg_reader, &mut msg_writer).await {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "handshake failed");
            std::process::exit(1);
        }
    };

    tracing::info!(node_id = %node_id, peers = ?node_ids, "node initialized");

    match node::run(config, node_id, node_ids, msg_reader, msg_writer).await {
        Ok(_) => tracing::info!("node shutdown successfully"),
        Err(e) => {
            tracing::error!(error = %e, "node run failed");
            std::process::exit(1);
        }
    }
}

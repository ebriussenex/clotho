use std::time::Duration;

use crate::{
    message::{Msg, Payload},
    node::{HandleErr, MessageReader, NodeErr},
};

mod message;
mod node;
mod storage;
mod transport;

pub struct StdinReader {
    lines: tokio::io::Lines<tokio::io::BufReader<tokio::io::Stdin>>,
}

//impl MessageReader for StdinReader {
//    async fn next(&mut self) -> Result<Option<Msg<Payload>>, HandleErr> {
//        match self.lines.next_line().await.map_err(HandleErr::ReadStdIn)? {
//            Some(line) => Ok(Some(
//                serde_json::from_str(&line).map_err(HandleErr::Deserialize)?,
//            )),
//            None => Ok(None),
//        }
//    }
//}
//
#[tokio::main]
async fn main() {
    //let _ = node::process(node::Config {}) .await
    //    .inspect_err(|e| println!("processing failed: {e}"));
}

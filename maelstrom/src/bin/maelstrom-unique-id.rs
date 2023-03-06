use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use gossip_glomers::Node;
use serde_json::json;

fn main() {
    let counter = AtomicU64::new(0);
    let node = &Node::new();
    node.run(|msg| match msg.body["type"].as_str().unwrap() {
        "generate" => {
            let curr = counter.fetch_add(1, SeqCst);
            let id = format!("{}{}", node.id, curr);
            node.reply(
                &msg,
                json!({
                    "type": "generate_ok",
                    "id": id
                }),
            )
        }
        ty => unreachable!("msg type {ty}"),
    });
}

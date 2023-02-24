use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use gossip_glomers::Node;
use serde_json::json;

fn main() {
    let mut node = Node::new();
    let mut counters: BTreeMap<String, u64> =
        node.node_ids.iter().cloned().map(|s| (s, 0)).collect();
    let mut last_broadcast = Instant::now();
    const TIMEOUT: Duration = Duration::from_millis(1000);

    loop {
        // Resend after 1s timeout
        let now = Instant::now();
        if last_broadcast < now {
            for id in node.node_ids.clone() {
                if id != node.id {
                    node.rpc(
                        id,
                        json!({
                            "type": "broadcast",
                            "counter": counters[&node.id]
                        }),
                    );
                }
            }
            last_broadcast = now + TIMEOUT;
        }
        if let Some(msg) = node.recv_timeout(last_broadcast.duration_since(now)) {
            match msg.body["type"].as_str().unwrap() {
                "broadcast" => {
                    counters.insert(msg.src.clone(), msg.body["counter"].as_u64().unwrap());
                    node.reply(
                        &msg,
                        json!({
                            "type": "broadcast_ok"
                        }),
                    );
                }
                "broadcast_ok" => {}
                "read" => node.reply(
                    &msg,
                    json!({
                        "type": "read_ok",
                        "value": counters.values().sum::<u64>()
                    }),
                ),
                "add" => {
                    counters
                        .entry(node.id.clone())
                        .and_modify(|c| *c += msg.body["delta"].as_u64().unwrap());
                    node.reply(
                        &msg,
                        json!({
                            "type": "add_ok"
                        }),
                    )
                }
                ty => unreachable!("msg type {ty}"),
            }
        }
    }
}

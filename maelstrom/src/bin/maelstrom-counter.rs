use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicI64, Ordering::SeqCst},
    thread::{scope, sleep},
    time::Duration,
};

use gossip_glomers::Node;
use serde_json::json;
fn main() {
    let node = &Node::new();
    let state: BTreeMap<String, AtomicI64> = node
        .node_ids
        .iter()
        .cloned()
        .map(|s| (s, AtomicI64::new(0)))
        .collect();

    scope(|s| {
        // Broadcast every 1 second
        s.spawn(|| loop {
            sleep(Duration::from_secs(1));
            let current = state[&node.id].load(SeqCst);
            for id in node.other_ids() {
                let node = &node;
                s.spawn(move || {
                    node.rpc(
                        id.clone(),
                        json!({
                            "type": "broadcast",
                            "counter": current
                        }),
                    )
                    .ok();
                });
            }
        });

        node.run(|msg| match msg.body["type"].as_str().unwrap() {
            "broadcast" => {
                let counter = msg.body["counter"].as_i64().unwrap();
                state[&msg.src].store(counter, SeqCst);
                node.reply(
                    &msg,
                    json!({
                        "type": "broadcast_ok"
                    }),
                );
            }
            "read" => {
                let sum = state.values().map(|i| i.load(SeqCst)).sum::<i64>();
                node.reply(
                    &msg,
                    json!({
                        "type": "read_ok",
                        "value": sum
                    }),
                )
            }
            "add" => {
                state[&node.id].fetch_add(msg.body["delta"].as_i64().unwrap(), SeqCst);
                node.reply(
                    &msg,
                    json!({
                        "type": "add_ok"
                    }),
                )
            }
            ty => unreachable!("msg type {ty}"),
        });
    });
}

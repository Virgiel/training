use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use gossip_glomers::Node;
use serde_json::json;

type Counters = BTreeMap<String, u64>;

fn main() {
    let mut node = Node::new(Counters::new());
    *node.state_mut() = node.node_ids.iter().cloned().map(|s| (s, 0)).collect();

    /// Broadcast every 1 sec

    fn broadcast(node: &mut Node<Counters>) {
        node.spawn(
            Box::new(|node, _| {
                for id in node.node_ids.clone() {
                    if id != node.id {
                        node.rpc(
                            id,
                            json!({
                                "type": "broadcast",
                                "counter": node.state()[&node.id]
                            }),
                            Box::new(|_, _| {}),
                        );
                    }
                }
                broadcast(node)
            }),
            Instant::now() + Duration::from_secs(1),
        )
    }
    broadcast(&mut node);

    loop {
        let msg = node.run();
        match msg.body["type"].as_str().unwrap() {
            "broadcast" => {
                node.state_mut()
                    .insert(msg.src.clone(), msg.body["counter"].as_u64().unwrap());
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
                    "value": node.state().values().sum::<u64>()
                }),
            ),
            "add" => {
                let key = node.id.clone();
                node.state_mut()
                    .entry(key)
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

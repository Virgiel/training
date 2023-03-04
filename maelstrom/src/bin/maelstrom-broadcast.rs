use std::{collections::BTreeSet, thread::scope};

use gossip_glomers::Node;
use parking_lot::{Mutex, RwLock};
use serde_json::json;

fn main() {
    let msgs = Mutex::new(BTreeSet::<u64>::new());
    let neighbours = RwLock::new(Vec::<String>::new());

    Node::new().run(|node, mut msg| match msg.body["type"].as_str().unwrap() {
        "broadcast" => {
            let message = msg.body["message"].as_u64().unwrap();
            if msgs.lock().insert(message) {
                scope(|s| {
                    for id in neighbours.read().clone() {
                        if id != msg.src {
                            s.spawn(move || loop {
                                if node
                                    .rpc(
                                        id.clone(),
                                        json!({
                                            "type": "broadcast",
                                            "message": message
                                        }),
                                    )
                                    .is_ok()
                                {
                                    break;
                                }
                            });
                        }
                    }
                })
            }
            node.reply(
                &msg,
                json!({
                    "type": "broadcast_ok"
                }),
            );
        }
        "read" => {
            let messages = msgs.lock().clone();
            node.reply(
                &msg,
                json!({
                    "type": "read_ok",
                    "messages": messages
                }),
            )
        }
        "topology" => {
            let new = serde_json::from_value(msg.body["topology"][&node.id].take()).unwrap();
            *neighbours.write() = new;
            node.reply(
                &msg,
                json!({
                    "type": "topology_ok"
                }),
            )
        }
        ty => unreachable!("msg type {ty}"),
    });
}

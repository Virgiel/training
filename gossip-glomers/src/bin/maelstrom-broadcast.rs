use std::collections::BTreeSet;

use gossip_glomers::Node;
use serde_json::json;

fn main() {
    let mut node = Node::new(());
    let mut msgs = BTreeSet::new();
    let mut neighbours: Vec<String> = Vec::new();

    fn broadcast(node: &mut Node<()>, message: u64, to: String) {
        node.rpc(
            to.clone(),
            json!({
                "type": "broadcast",
                "message": message
            }),
            Box::new(move |node, result| {
                if result.is_err() {
                    broadcast(node, message, to)
                }
            }),
        )
    }

    loop {
        let mut msg = node.run();
        match msg.body["type"].as_str().unwrap() {
            "broadcast" => {
                let message = msg.body["message"].as_u64().unwrap();
                if msgs.insert(message) {
                    for id in &neighbours {
                        if *id != msg.src {
                            broadcast(&mut node, message, id.clone());
                        }
                    }
                }
                node.reply(
                    &msg,
                    json!({
                        "type": "broadcast_ok"
                    }),
                );
            }
            "read" => {
                let messages = msgs.clone();
                node.reply(
                    &msg,
                    json!({
                        "type": "read_ok",
                        "messages": messages
                    }),
                )
            }
            "topology" => {
                neighbours = serde_json::from_value(msg.body["topology"][&node.id].take()).unwrap();
                node.reply(
                    &msg,
                    json!({
                        "type": "topology_ok"
                    }),
                )
            }
            ty => unreachable!("msg type {ty}"),
        }
    }
}

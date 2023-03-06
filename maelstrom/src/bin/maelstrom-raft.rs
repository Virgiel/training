use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Index,
    thread::{scope, sleep, Scope},
    time::{Duration, Instant},
};

use gossip_glomers::Node;
use gossip_glomers::{Err, Msg};
use parking_lot::Mutex;
use serde_json::{json, Value};

struct StateMachine {
    db: BTreeMap<String, Value>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            db: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, op: &Value) -> Value {
        let key = op["key"].as_u64().unwrap().to_string();
        match op["type"].as_str().unwrap() {
            "read" => {
                let value = self.db.get(&key);
                if let Some(value) = value {
                    json!({
                        "type": "read_ok",
                        "value": value
                    })
                } else {
                    Err::KeyDoesNotExist.msg()
                }
            }
            "write" => {
                let value = op["value"].clone();
                self.db.insert(key, value);
                json!({"type": "write_ok"})
            }
            "cas" => {
                let from = op["from"].clone();
                let to = op["to"].clone();
                if let Some(value) = self.db.get(&key) {
                    if value == &from {
                        self.db.insert(key, to);
                        json!({ "type": "cas_ok" })
                    } else {
                        Err::PreconditionFailed.msg()
                    }
                } else {
                    Err::KeyDoesNotExist.msg()
                }
            }
            ty => unreachable!("op type {ty}"),
        }
    }
}

pub fn majority(n: usize) -> usize {
    n / 2 + 1
}

pub fn median(mut items: Vec<u64>) -> u64 {
    items.sort_unstable();
    items[items.len() - majority(items.len())]
}

struct Log {
    entries: Vec<(u64, Msg)>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![(
                0,
                Msg {
                    src: String::new(),
                    dest: String::new(),
                    body: Value::Null,
                },
            )],
        }
    }

    pub fn append(&mut self, entries: impl IntoIterator<Item = (u64, Msg)>) {
        self.entries.extend(entries);
    }

    pub fn last(&self) -> &(u64, Msg) {
        self.entries.last().unwrap()
    }

    pub fn size(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn from_index(&self, idx: u64) -> &[(u64, Msg)] {
        &self.entries[idx as usize - 1..]
    }

    pub fn truncate(&mut self, len: u64) {
        self.entries.drain(len as usize..);
    }

    pub fn get(&self, idx: u64) -> Option<&(u64, Msg)> {
        self.entries.get(idx as usize - 1)
    }
}

impl Index<u64> for Log {
    type Output = (u64, Msg);

    fn index(&self, index: u64) -> &Self::Output {
        &self.entries[index as usize - 1]
    }
}

const ELECTION_TIMEOUT: u64 = 2000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);
const MIN_REPLICATION_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, PartialEq, Eq)]
enum State {
    Leader,
    Candidate,
    Follower,
}
struct Raft {
    election_deadline: Instant,
    step_down_deadline: Instant,
    last_replication: Instant,
    state: State,
    last_applied: u64,
    term: u64,
    log: Log,
    voted_for: Option<String>,
    machine: StateMachine,
    next_index: BTreeMap<String, u64>,
    match_index: BTreeMap<String, u64>,
    commit_index: u64,
    leader: Option<String>,
}

impl Raft {
    pub fn new() -> Self {
        Self {
            election_deadline: Instant::now(),
            step_down_deadline: Instant::now(),
            state: State::Follower,
            commit_index: 0,
            term: 0,
            log: Log::new(),
            voted_for: None,
            machine: StateMachine::new(),
            last_replication: Instant::now(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            last_applied: 1,
            leader: None,
        }
    }

    pub fn become_candidate<'a, 'b>(
        &mut self,
        s: &'b Scope<'b, 'a>,
        node: &'a Node,
        raft: &'b Mutex<Raft>,
    ) {
        self.state = State::Candidate;
        self.advance_term(self.term + 1);
        self.voted_for = Some(node.id.to_owned());
        self.leader = None;
        self.reset_election_deadline();
        self.request_vote(s, node, raft);
        eprintln!("Became candidate for term {}", self.term);
    }

    pub fn become_follower(&mut self) {
        self.state = State::Follower;
        self.match_index.clear();
        self.next_index.clear();
        self.leader = None;
        self.reset_election_deadline();
        eprintln!("Became follower for term {}", self.term)
    }

    pub fn become_leader(&mut self, node: &Node) {
        assert_eq!(self.state, State::Candidate);
        self.state = State::Leader;
        self.leader = None;
        self.next_index = BTreeMap::from_iter(
            node.other_ids()
                .map(|i| (i.to_owned(), self.log.size() + 1)),
        );
        self.match_index = BTreeMap::from_iter(node.other_ids().map(|i| (i.to_owned(), 0)));
        self.reset_step_down_deadline();
        eprintln!("Became leader for term {}", self.term)
    }

    pub fn maybe_step_down(&mut self, remote_term: u64) {
        if self.term < remote_term {
            eprintln!(
                "Stepping down: remote term {remote_term} higher than our term {}",
                self.term
            );
            self.advance_term(remote_term);
            self.become_follower();
        }
    }

    pub fn on_request_vote(&mut self, msg: &Msg) -> Value {
        let term = msg.body["term"].as_u64().unwrap();
        let candidate_id = msg.body["candidate_id"].as_str().unwrap();
        let remote_last_log_term = msg.body["last_log_term"].as_u64().unwrap();
        let remote_last_log_index = msg.body["last_log_index"].as_u64().unwrap();
        let last_log_term = self.log.last().0;
        let last_log_index = self.log.size();
        self.maybe_step_down(term);
        let mut granted = false;
        if term < self.term {
            eprintln!(
                "Candidate term {term} lower than {}, not granting vote.",
                self.term
            );
        } else if let Some(vote) = &self.voted_for {
            eprintln!("Already voted for {vote}; not granting vote.");
        } else if remote_last_log_term < last_log_term {
            eprintln!("Have log entries from term {last_log_term}, which is newer than remote term {remote_last_log_term}; not granting vote.");
        } else if remote_last_log_term == last_log_term && remote_last_log_index < last_log_index {
            eprintln!("Our logs are both at term {last_log_term}, but our log is {last_log_index} and theirs is only {remote_last_log_index} long; not granting vote.");
        } else {
            eprintln!("Granting vote to {candidate_id}");
            granted = true;
            self.voted_for = Some(candidate_id.to_owned());
            self.reset_election_deadline();
        }

        json!({
            "type": "request_vote_res",
            "term": term,
            "vote_granted": granted
        })
    }

    pub fn request_vote<'a, 'b>(
        &mut self,
        s: &'b Scope<'b, 'a>,
        node: &'a Node,
        raft: &'b Mutex<Raft>,
    ) {
        let body = json!({
            "type": "request_vote",
            "term": self.term,
            "candidate_id": node.id,
            "last_log_index": self.log.size(),
            "last_log_term": self.log.last().0
        });
        s.spawn(move || {
            let votes = &Mutex::new(BTreeSet::from_iter([node.id.to_owned()]));
            let body = &body;
            scope(|s| {
                for id in node.other_ids() {
                    s.spawn(move || {
                        if let Ok(res) = node.rpc(id.to_owned(), body.clone()) {
                            let term = res.body["term"].as_u64().unwrap();
                            let vote_granted = res.body["vote_granted"].as_bool().unwrap();
                            let mut votes = votes.lock();
                            let mut r = raft.lock();
                            r.maybe_step_down(term);
                            r.reset_step_down_deadline();
                            if r.state == State::Candidate && r.term == term && vote_granted {
                                votes.insert(id.to_owned());
                                eprintln!("Have votes: {votes:?} for term {term}");

                                if majority(node.node_ids.len()) <= votes.len() {
                                    r.become_leader(node);
                                }
                            }
                        }
                    });
                }
            })
        });
    }

    pub fn replicate_log<'a, 'b>(
        &mut self,
        s: &'b Scope<'b, 'a>,
        node: &'a Node,
        raft: &'b Mutex<Raft>,
    ) {
        let elapsed_time = self.last_replication.elapsed();
        let mut replicated = false;
        if self.state == State::Leader && MIN_REPLICATION_INTERVAL < elapsed_time {
            for id in node.other_ids() {
                let ni = self.next_index[id];
                let entries = self.log.from_index(ni);
                if !entries.is_empty() || HEARTBEAT_INTERVAL < elapsed_time {
                    eprintln!("Replicating {ni}+ to {id} for {}", self.term);
                    replicated = true;
                    let body = json!({
                        "type": "append_entries",
                        "term": self.term,
                        "leader_id": node.id,
                        "prev_log_index": ni-1,
                        "prev_log_term": self.log[ni-1].0,
                        "entries": entries,
                        "leader_commit": self.commit_index
                    });
                    let len = entries.len() as u64;
                    s.spawn(move || {
                        if let Ok(msg) = node.rpc(id.to_owned(), body) {
                            let term = msg.body["term"].as_u64().unwrap();
                            let mut r = raft.lock();
                            r.maybe_step_down(term);
                            if r.state == State::Leader && r.term == term {
                                r.reset_step_down_deadline();
                                if msg.body["success"].as_bool().unwrap() {
                                    *r.next_index.get_mut(id).unwrap() =
                                        r.next_index[id].max(ni + len);
                                    *r.match_index.get_mut(id).unwrap() =
                                        r.match_index[id].max(ni + len - 1);
                                } else {
                                    *r.next_index.get_mut(id).unwrap() -= 1;
                                }
                                eprintln!(
                                    "Next index {:?} for term {} {} {:?}",
                                    r.next_index, r.term, node.id, r.state
                                );
                                r.advance_commit_index(node);
                            }
                        }
                    });
                }
            }
            if replicated {
                self.last_replication = Instant::now();
            }
        }
    }

    pub fn advance_term(&mut self, term: u64) {
        assert!(self.term <= term, "Term can't go backward");
        self.term = term;
        self.voted_for = None;
    }

    pub fn tick_deadline<'a, 'b>(
        &mut self,
        s: &'b Scope<'b, 'a>,
        node: &'a Node,
        raft: &'b Mutex<Raft>,
    ) {
        if self.election_deadline < Instant::now() {
            if self.state != State::Leader {
                self.become_candidate(s, node, raft)
            } else {
                self.reset_election_deadline()
            }
        }
    }

    pub fn reset_election_deadline(&mut self) {
        self.election_deadline = Instant::now()
            + Duration::from_millis(fastrand::u64(ELECTION_TIMEOUT..ELECTION_TIMEOUT * 2))
    }

    pub fn tick_step_down(&mut self) {
        if self.state == State::Leader && self.step_down_deadline < Instant::now() {
            self.become_follower();
        }
    }

    pub fn reset_step_down_deadline(&mut self) {
        self.step_down_deadline = Instant::now() + Duration::from_millis(ELECTION_TIMEOUT)
    }

    pub fn advance_state_machine(&mut self, node: &Node) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let msg = &self.log[self.last_applied].1;
            let res = self.machine.apply(&msg.body);
            if self.state == State::Leader {
                node.reply(msg, res);
            }
        }
    }

    pub fn advance_commit_index(&mut self, node: &Node) {
        if self.state == State::Leader {
            let n = median(
                self.match_index
                    .values()
                    .map(|i| *i)
                    .chain([self.log.size()])
                    .collect(),
            );
            if n > self.commit_index && self.log[n].0 == self.term {
                eprintln!("Commit index {n} for term {}", self.term);
                self.commit_index = n;
            }
        }
        self.advance_state_machine(node);
    }
}

fn main() {
    let raft = Mutex::new(Raft::new());
    let node = &Node::new();
    scope(|s| {
        s.spawn(|| loop {
            sleep(Duration::from_millis(fastrand::u64(100..200)));
            raft.lock().tick_deadline(s, node, &raft);
        });
        s.spawn(|| loop {
            sleep(Duration::from_millis(100));
            raft.lock().tick_step_down();
        });
        s.spawn(|| loop {
            sleep(MIN_REPLICATION_INTERVAL);
            raft.lock().replicate_log(s, node, &raft)
        });
        node.run(|mut msg| match msg.body["type"].as_str().unwrap() {
            "read" | "write" | "cas" => {
                let forward = {
                    let mut r = raft.lock();
                    if r.state == State::Leader {
                        let term = r.term;
                        r.log.append([(term, msg.clone())]);
                        None
                    } else {
                        Some(r.leader.clone())
                    }
                };
                if let Some(forward) = forward {
                    let res = if let Some(leader) = forward {
                        match node.rpc(leader, msg.body.clone()) {
                            Ok(res) => res.body,
                            Err(e) => e.msg(),
                        }
                    } else {
                        Err::TemporarilyUnavailable.msg()
                    };
                    node.reply(&msg, res);
                }
            }
            "request_vote" => {
                let result = raft.lock().on_request_vote(&msg);
                node.reply(&msg, result);
            }
            "append_entries" => {
                let mut success = true;
                let term = msg.body["term"].as_u64().unwrap();
                let prev_log_index = msg.body["prev_log_index"].as_u64().unwrap();
                let commit_index = msg.body["leader_commit"].as_u64().unwrap();
                let leader_id = msg.body["leader_id"].as_str().unwrap().to_owned();
                let entries: Vec<(u64, Msg)> =
                    serde_json::from_value(msg.body["entries"].take()).unwrap();
                {
                    let mut lock = raft.lock();
                    lock.maybe_step_down(term);
                    if term == lock.term {
                        lock.leader = Some(leader_id);
                        lock.reset_election_deadline();
                        if let Some(prev) = &lock.log.get(prev_log_index) {
                            if prev.0 == lock.term {
                                lock.log.truncate(prev_log_index);
                                lock.log.append(entries);
                                if lock.commit_index < commit_index {
                                    lock.commit_index = lock.log.size().min(commit_index);
                                    lock.advance_state_machine(node);
                                }
                                success = true;
                            }
                        }
                    }
                };
                node.reply(
                    &msg,
                    json!({
                        "type": "append_entries_res",
                        "term": term,
                        "success": success
                    }),
                )
            }
            ty => unreachable!("msg type {ty}"),
        });
    });
}

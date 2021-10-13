use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;

use futures::channel::mpsc::UnboundedSender;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use futures::executor::ThreadPool;
use rand::Rng;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Default, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub command: Vec<u8>,
}

pub enum RpcReply {
    RequestVoteReply(RequestVoteReply),
    AppendEntriesReply(AppendEntriesReply),
}

impl RpcReply {
    fn term(&self) -> u64 {
        match self {
            RpcReply::RequestVoteReply(x) => x.term,
            RpcReply::AppendEntriesReply(x) => x.term,
        }
    }
}

impl LogEntry {
    pub fn new(term: u64, command: Vec<u8>) -> LogEntry {
        LogEntry { term, command }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    current_term: u64,                   // latest term server has seen
    voted_for: Option<u64>,              // candidateId that received vote in current term
    log: Vec<LogEntry>,                  // first index is 1
    commit_index: u64,                   // index of highest log entry known to be commited
    last_applied: u64,                   // index of highest log entry applied to state machine
    next_index: Vec<u64>,                // for each server, index of the next log entry to send to
    match_index: Vec<u64>, // for each server, index of highest log entry known to be replicated
    apply_ch: UnboundedSender<ApplyMsg>, //
    election_start_t: u128, // election start if no heatbeat received
    election_timeout_t: u128, // election timeout and go to next term
    boot_t: Instant,       // the boot time of node
    role: Role,            // three type role
    vote_count: u64,       // granted vote received
    rpc_tx: Option<Sender<RpcReply>>, // peer clone Sender to send RPC reply
    heartbeat_expire_t: Vec<u128>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::new(),
            match_index: Vec::new(),

            apply_ch,
            election_start_t: 0,
            election_timeout_t: 0,
            boot_t: Instant::now(),
            role: Role::Follower,
            vote_count: 0,
            rpc_tx: None,
            heartbeat_expire_t: Vec::new(),
        };

        // rf.log.push(LogEntry::new(0, Vec::new()));
        // rf.next_index.resize(rf.peers.len(), 1);
        // rf.match_index.resize(rf.peers.len(), 0);

        // initialize from state persisted before a crash
        rf.last_applied = 0;
        rf.apply_ch.close_channel();
        rf.set_follower();
        rf.restore(&raft_state);

        rf
    }

    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn last_log_term(&self) -> u64 {
        if let Some(log) = self.log.last() {
            log.term
        } else {
            0
        }
    }
    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(&self, server: usize, args: RequestVoteArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let candidate = self.me;
        if let Some(tx) = self.rpc_tx.clone() {
            peer.spawn(async move {
                let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    if tx.send(RpcReply::RequestVoteReply(res)).is_err() {
                        warn!("@{} send request vote reply failed", server);
                    } else {
                        info!("@{} send request vote reply to {}", server, candidate);
                    }
                }
            })
        }
    }

    fn send_append_entries(&self, server: usize, args: AppendEntriesArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        if let Some(tx) = self.rpc_tx.clone() {
            peer.spawn(async move {
                let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    if tx.send(RpcReply::AppendEntriesReply(res)).is_err() {
                        warn!("@{} send append entries reply failed", server);
                    }
                }
            });
        }
    }

    fn heartbeat(&mut self) {
        for i in 0..self.peers.len() {
            if i != self.me {
                let current_time = self.current_time();
                if current_time > self.heartbeat_expire_t[i] {
                    self.heartbeat_expire_t[i] = current_time;
                    self.send_append_entries(
                        i,
                        AppendEntriesArgs {
                            term: self.current_term,
                            leader_id: self.me as u64,
                            prev_log_term: self.last_log_term(),
                            prev_log_index: self.last_log_index(),
                            entries_term: vec![],
                            entries_command: vec![],
                            leader_commit: self.commit_index,
                        },
                    )
                }
            }
        }
    }

    fn on_request_vote(&mut self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        if args.term > self.current_term {
            self.set_follower();
            self.current_term = args.term;
        }
        match self.role {
            Role::Follower => {
                let vote_granted = match self.voted_for {
                    None => {
                        args.last_log_term >= self.last_log_term()
                            || args.last_log_index >= self.last_log_index()
                                && args.last_log_term == self.last_log_term()
                    }
                    Some(candidate_id) => candidate_id == args.candidate_id,
                };
                if vote_granted {
                    self.voted_for = Some(args.candidate_id);
                }

                labrpc::Result::Ok(RequestVoteReply {
                    term: self.current_term,
                    vote_granted,
                    id: self.me as u64,
                })
            }
            _ => labrpc::Result::Ok(RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
                id: self.me as u64,
            }),
        }
    }

    fn on_append_entries(&mut self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        if args.term >= self.current_term {
            self.set_follower();
            self.current_term = args.term;
            labrpc::Result::Ok(AppendEntriesReply {
                follower_id: self.me as u64,
                success: true,
                term: self.current_term,
            })
        } else {
            labrpc::Result::Ok(AppendEntriesReply {
                follower_id: self.me as u64,
                success: false,
                term: self.current_term,
            })
        }
    }

    fn current_time(&self) -> u128 {
        self.boot_t.elapsed().as_millis()
    }

    fn set_follower(&mut self) {
        self.role = Role::Follower;
        self.election_start_t = self.current_time() + rand::thread_rng().gen_range(150, 300);
        self.voted_for = None;
    }

    fn set_candidate(&mut self) {
        self.role = Role::Candidate;
        self.election_timeout_t = self.current_time() + rand::thread_rng().gen_range(150, 300);
        self.election();
    }

    fn set_leader(&mut self) {
        self.role = Role::Leader;
        self.heartbeat_expire_t = Vec::new();
        let len = self.peers.len();
        self.heartbeat_expire_t.resize(len, self.current_time());
        self.match_index = Vec::new();
        self.match_index.resize(len, 0);
        self.next_index = Vec::new();
        self.next_index.resize(len, self.last_log_index() + 1);
        self.heartbeat();
    }

    fn election(&mut self) {
        self.vote_count = 1;
        self.voted_for = Some(self.me as u64);
        self.current_term += 1;
        for peer in 0..self.peers.len() {
            self.send_request_vote(
                peer,
                RequestVoteArgs {
                    term: self.current_term,
                    candidate_id: self.me as u64,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            );
        }
    }

    fn tick(&mut self) {
        let current_time = self.boot_t.elapsed().as_millis();
        match self.role {
            Role::Follower => {
                if current_time > self.election_start_t {
                    self.set_candidate();
                }
            }
            Role::Candidate => {
                if current_time > self.election_timeout_t {
                    self.set_candidate();
                }
            }
            Role::Leader => {
                self.heartbeat();
            }
        }
    }

    fn rx_handler(&mut self, reply: RpcReply) {
        if reply.term() > self.current_term {
            self.set_follower();
            self.current_term = reply.term();
        }
        if let RpcReply::RequestVoteReply(reply) = reply {
            if let Role::Candidate = self.role {
                if reply.vote_granted {
                    self.vote_count += 1;
                    if self.vote_count > (self.peers.len() / 2) as u64 {
                        self.set_leader();
                    }
                }
            }
        } else if let RpcReply::AppendEntriesReply(reply) = reply {
            if let Role::Leader = self.role {
                if reply.success {
                    let id = reply.follower_id as usize;
                    self.next_index[id] += 1;
                    self.match_index[id] = self.next_index[id] - 1;
                } else if reply.term > self.current_term {
                    self.set_follower();
                } else {
                    let id = reply.follower_id as usize;
                    self.next_index[id] -= 1;
                    self.send_append_entries(
                        id,
                        AppendEntriesArgs {
                            term: self.current_term,
                            leader_id: self.me as u64,
                            prev_log_term: self.last_log_term(),
                            prev_log_index: self.last_log_index(),
                            entries_term: vec![],
                            entries_command: vec![],
                            leader_commit: self.commit_index,
                        },
                    )
                }
            }
        }
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Option<Raft>>>,
    killed: Arc<AtomicBool>,
    ticker: Arc<Option<JoinHandle<()>>>,
    rx_handler: Arc<Option<JoinHandle<()>>>,
    executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let me = raft.me;
        let (tx, rx) = channel();
        raft.rpc_tx = Some(tx);
        let raft = Arc::new(Mutex::new(Some(raft)));

        let mut node = Node {
            raft,
            killed: Arc::new(AtomicBool::new(false)),
            ticker: Arc::new(None),
            rx_handler: Arc::new(None),
            executor: ThreadPool::new().expect("failed to build threadpool"),
        };
        // initiate ticker to check election timeout
        let raft = node.raft.clone();
        let killed = node.killed.clone();
        node.ticker = Arc::new(Some(std::thread::spawn(move || {
            info!("@{} start ticking", me);
            while !killed.load(SeqCst) {
                {
                    let mut raft = raft.lock().unwrap();
                    if let Some(raft) = raft.as_mut() {
                        raft.tick();
                    } else {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            info!("@{} stop ticking", me);
        })));

        // initiate rx_handler to handler rpc reply
        let raft = node.raft.clone();
        let killed = node.killed.clone();
        node.rx_handler = Arc::new(Some(std::thread::spawn(move || {
            info!("@{} start rx_handler", me);
            for reply in rx.iter() {
                if killed.load(SeqCst) {
                    break;
                }
                let mut raft = raft.lock().unwrap();
                if let Some(raft) = raft.as_mut() {
                    raft.rx_handler(reply);
                }
                std::thread::yield_now();
            }
            info!("@{} stop rx_handler", me);
        })));
        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        if !self.is_leader() {
            Result::Err(Error::NotLeader)
        } else if let Some(raft) = self.raft.lock().unwrap().as_mut() {
            raft.start(command)
        } else {
            Result::Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        let mut raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            raft.current_term
        } else {
            0
        }
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        let mut raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            matches!(raft.role, Role::Leader)
        } else {
            false
        }
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let raft = self.raft.clone();
        let mut raft = raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            raft.on_request_vote(args)
        } else {
            labrpc::Result::Err(labrpc::Error::Stopped)
        }
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let raft = self.raft.clone();
        let mut raft = raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            raft.on_append_entries(args)
        } else {
            labrpc::Result::Err(labrpc::Error::Stopped)
        }
    }
}

labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
        rpc append_entries(RequestEntryArgs) returns (RequestEntryReply);
        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
    }
}
pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(uint64, tag="2")]
    pub candidate_id: u64,

    #[prost(uint64, tag="3")]
    pub last_log_index: u64,

    #[prost(uint64, tag="4")]
    pub last_log_term: u64,
}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(bool, tag="2")]
    pub vote_granted: bool,

    //#[prost(uint64, tag="3")]
    //pub last_log_index: u64,     //返回false时，如果有节点有更加新的log，虽然没提交，但是优先那个节点为leader，测试相关    
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestEntryArgs {
    // Your data here (2A).
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(uint64, tag="2")]
    pub leader_id: u64,

    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,

    #[prost(uint64, tag = "4")]
    pub prev_log_term: u64,

    #[prost(bytes, repeated, tag = "5")]
    pub entries: Vec<Vec<u8>>,   //发送LogEntry，一次Vec<Vec>发送多个

    #[prost(uint64, tag = "6")]
    pub leader_commit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestEntryReply {
    // Your data here (2A).
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(bool, tag="2")]
    pub success: bool,

    #[prost(uint64, tag="3")]
    pub next_index: u64,
}
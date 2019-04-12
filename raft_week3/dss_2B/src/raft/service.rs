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

    #[prost(bytes, tag = "5")]
    pub entries: Vec<u8>,   //暂时一次发送一个，后面可Vec<Vec>发送多个

    #[prost(uint64, tag = "6")]  //entries_term是entries的任期号，理论上entries是整个结构，增加一个参数表示任期，
    pub entries_term: u64,

    #[prost(uint64, tag = "7")]
    pub leader_commit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RequestEntryReply {
    // Your data here (2A).
    #[prost(uint64, tag="1")]
    pub term: u64,

    #[prost(bool, tag="2")]
    pub success: bool,
}
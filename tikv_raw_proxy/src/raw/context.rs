
use kvproto::{kvrpcpb, metapb};
use crate::Key;

/*
metapb:
message Region {
    uint64 id = 1;
    // Region key range [start_key, end_key).
    bytes start_key = 2;
    bytes end_key = 3;
    RegionEpoch region_epoch = 4;
    repeated Peer peers = 5;
}
message Peer {      
    uint64 id = 1;
    uint64 store_id = 2;
    bool is_learner = 3;
}
message RegionEpoch {
    // Conf change version, auto increment when add or remove peer
    uint64 conf_ver = 1;
    // Region version, auto increment when split or merge
    uint64 version = 2;
}
enum StoreState {
    Up = 0;
    Offline = 1;
    Tombstone = 2;
}

// Case insensitive key/value for replica constraints.
message StoreLabel {
    string key = 1;
    string value = 2;
}

message Store {
    uint64 id = 1;
    string address = 2;
    StoreState state = 3;
    repeated StoreLabel labels = 4;
    string version = 5;
    // more attributes......
}
kvrpcpb:
message Context {
    reserved 4;
    reserved "read_quorum";
    uint64 region_id = 1;
    metapb.RegionEpoch region_epoch = 2;
    metapb.Peer peer = 3;
    uint64 term = 5;
    CommandPri priority = 6;
    IsolationLevel isolation_level = 7;
    bool not_fill_cache = 8;
    bool sync_log = 9;
    bool handle_time = 10; // true means return handle time detail
    bool scan_detail = 11; // true means return scan cf's detail
}
*/

#[derive(Clone, Debug)]
pub struct RawContext {
    pub region: metapb::Region,
    pub leader: metapb::Peer,
    pub store: metapb::Store,
}

impl RawContext {
    pub fn address(&self) -> String {
        self.store.get_address().clone().to_string()
    }
    pub fn start_key(&self) -> Key {
        self.region.get_start_key().clone().to_vec()
    }
    pub fn end_key(&self) -> Key {
        self.region.get_end_key().clone().to_vec()
    }
}

impl From<RawContext> for kvrpcpb::Context {
    fn from(mut ctx: RawContext) -> kvrpcpb::Context {
        let mut kvctx = kvrpcpb::Context::new();
        kvctx.set_region_id(ctx.region.id);
        kvctx.set_region_epoch(ctx.region.take_region_epoch());
        kvctx.set_peer(ctx.leader);
        kvctx
    }
}
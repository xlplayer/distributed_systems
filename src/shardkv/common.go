package shardkv
import "../shardmaster"
//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNeedWait    = "ErrNeedWait"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid int64
	Seq int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int32
}

type GetReply struct {
	Err   Err
	Value string
}

type PushArgs struct{
	Shards []int
	Shard_cid_seq [shardmaster.NShards] map[int64] int32
	Db	map[string]string
	Gid int
	Num int
}

type PushReply struct{
	Err	Err
}
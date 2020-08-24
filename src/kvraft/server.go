package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"time"
	"bytes"
	"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	Cid int64
	Seq int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	persister *raft.Persister
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string] string
	cid_seq map[int64] int32
	agreeCh map[int] chan Op
}

func (kv *KVServer) encodeSnapshot() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.cid_seq)
	e.Encode(kv.db)
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte){
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cid_seq map[int64] int32
	var db map[string] string
	if d.Decode(&cid_seq) != nil ||
	    d.Decode(&db) != nil{
	    DPrintf("decode error!")
	} else {
		kv.mu.Lock()
		kv.cid_seq = cid_seq
		kv.db = db
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("%v start to Get",kv.me)
	// Your code here.
	kv.mu.Lock()
	seq, ok:= kv.cid_seq[args.Cid]
	kv.mu.Unlock()
	if ok&&seq>=args.Seq{
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		return
	}
	cmd := Op{"Get",args.Key,"",args.Cid,args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	if(!isLeader){
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getAgreeCh(index)
	select{
	case op := <-ch:
		close(ch)
		if cmd==op{
			kv.mu.Lock()
			var ok bool
			reply.Value,ok = kv.db[args.Key]
			kv.mu.Unlock()
			if !ok{
				reply.Err = ErrNoKey
			}else{
				reply.Err = OK
			}
		}else{
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	DPrintf("%v end to Get",kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%v start to PuAppend",kv.me)
	kv.mu.Lock()
	seq, ok:= kv.cid_seq[args.Cid]
	kv.mu.Unlock()
	if ok&&seq>=args.Seq{
		reply.Err = OK
		return
	}
	cmd := Op{args.Op,args.Key,args.Value,args.Cid,args.Seq}
	index, _, isLeader := kv.rf.Start(cmd)
	if(!isLeader){
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getAgreeCh(index)
	select{
	case op := <-ch:
		close(ch)
		if cmd==op{
			reply.Err = OK
		}else{
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	DPrintf("%v end to PuAppend",kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly,StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string] string)
	kv.cid_seq = make(map[int64] int32)
	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.agreeCh = make(map[int] chan Op)
	go kv.waitAgree()
	return kv
}

func (kv *KVServer) getAgreeCh(idx int) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.agreeCh[idx]
	if(!ok){
		ch = make(chan Op, 1)
		kv.agreeCh[idx] = ch
	}
	return ch
}

func (kv *KVServer) waitAgree(){
	for{
		if kv.killed(){
			break
		} 
		msg := <-kv.applyCh
		if !msg.CommandValid{
			kv.decodeSnapshot(msg.Command.([]byte))
		}else{
			op := msg.Command.(Op)
			kv.mu.Lock()
			maxseq, ok := kv.cid_seq[op.Cid]
			if !ok || op.Seq > maxseq{
				kv.cid_seq[op.Cid] = op.Seq
				if op.Op=="Put"{
					kv.db[op.Key] = op.Value
				}else if op.Op=="Append"{
					kv.db[op.Key] += op.Value
				}
			}
			if kv.maxraftstate!=-1 && kv.persister.RaftStateSize() >= kv.maxraftstate*9/10{
				//DPrintf("currentsize{%v} maxraftstate{%v}",kv.persister.RaftStateSize(), kv.maxraftstate)
				go kv.rf.SaveSnapshot(msg.CommandIndex, kv.encodeSnapshot())
			}
			kv.mu.Unlock()
			kv.getAgreeCh(msg.CommandIndex) <- op
		}
	}
}

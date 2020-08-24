package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader int
	id int64
	seq int32
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.id = nrand()
	ck.seq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	curSeq := atomic.AddInt32(&ck.seq, 1)
	args := GetArgs{key,ck.id,curSeq}
	reply := GetReply{}

	i := ck.leader
	for {
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok&& reply.Err!=ErrWrongLeader{
			//Dprintf("Get success")
			ck.leader = i
			if reply.Err==OK{
				return reply.Value
			}else{
				return ""
			}

		}else{
			i = (i+1)%len(ck.servers)
		}
	}
	//return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	curSeq := atomic.AddInt32(&ck.seq, 1)
	args := PutAppendArgs{key,value,op,ck.id,curSeq}
	reply := PutAppendReply{}

	i := ck.leader
	for {
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok&& reply.Err!=ErrWrongLeader{
			//Dprintf("PutAppend success")
			ck.leader = i
			break
		}else{
			i = (i+1)%len(ck.servers)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

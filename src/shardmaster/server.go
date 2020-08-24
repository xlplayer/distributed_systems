package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "sync/atomic"
import "time"
import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	cid_seq map[int64] int32
	agreeCh map[int] chan Op
	dead    int32 // set by Kill()
}


type Op struct {
	// Your data here.
	Op string
	Seq int32
	Cid int64
	Servers map[int][]string
	GIDs []int
	Shard int
	GID int
	Num int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	servers := make(map[int][]string)
	for k,v := range args.Servers{
		servers[k] = make([]string, len(v))
		for i:=0;i<len(v);i++{
			servers[k][i] = v[i]
		}
	}
	op := Op{Op:"Join",Servers:servers, Cid:args.Cid, Seq:args.Seq}
	err, _ := sm.StartCommand(op)

	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	gid := make([]int, len(args.GIDs))
	for i := 0; i < len(args.GIDs); i++ {
		gid[i] = args.GIDs[i]
	}
	op := Op{Op:"Leave", GIDs:gid, Cid:args.Cid, Seq:args.Seq}
	err, _ := sm.StartCommand(op)

	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Op:"Move", Shard:args.Shard, GID:args.GID, Cid:args.Cid, Seq:args.Seq}
	err, _ := sm.StartCommand(op)

	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Op:"Query", Num: args.Num, Cid:args.Cid, Seq:args.Seq}
	err, c := sm.StartCommand(op)

	reply.Config = c
	reply.Err = err
	if err == ErrWrongLeader{
		reply.WrongLeader = true
	}else{
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) StartCommand(op Op) (Err, Config){
	// Your code here.
	DPrintf("Get op {%v}",op)
	c := sm.MakeEmptyConfig()
	sm.mu.Lock()
	seq, ok:= sm.cid_seq[op.Cid]
	if ok&&seq>=op.Seq{
		if op.Op == "Query"{
			num := op.Num
			if op.Num <0 || op.Num >=len(sm.configs){
				num = len(sm.configs)-1
			}
			sm.CopyConfig(&sm.configs[num],&c)
		}
		sm.mu.Unlock()
		return OK, c
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader{
		sm.mu.Unlock()
		return ErrWrongLeader, c
	}
	sm.mu.Unlock()
	ch := sm.getAgreeCh(index)
	select{
	case applyedop := <-ch:
		close(ch)
		if applyedop.Cid == op.Cid && applyedop.Seq == op.Seq {
			if op.Op == "Query"{
				sm.mu.Lock()
				num := op.Num
				if op.Num <0 || op.Num >=len(sm.configs){
					num = len(sm.configs)-1
				}
				sm.CopyConfig(&sm.configs[num],&c)
				sm.mu.Unlock()
			}
			DPrintf("get config {%v}",c)
			return OK, c
		} else{
			return ErrWrongLeader, c
		}
	case <-time.After(500*time.Millisecond):
		return ErrWrongLeader, c
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0] = sm.MakeEmptyConfig()

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.cid_seq = make(map[int64] int32)
	sm.agreeCh = make(map[int] chan Op)
	go sm.waitAgree()
	return sm
}

func (sm *ShardMaster) MakeEmptyConfig() Config{
	c := Config{}
	c.Num = 0
	for i := 0; i < NShards; i++ {
		c.Shards[i] = 0
	}
	c.Groups = map[int][]string{}
	return c
}

func (sm *ShardMaster) CopyConfig(from *Config, to *Config){
	if to == nil {
		return
	}
	to.Num = from.Num
	for i := 0; i < NShards; i++ {
		to.Shards[i] = from.Shards[i]
	}
	to.Groups = make(map[int][]string)
	for k, v := range from.Groups {
		to.Groups[k] = make([]string, len(v))
		for i := 0; i < len(v); i++ {
			to.Groups[k][i] = v[i]
		}
	}
}

func (sm *ShardMaster) getAgreeCh(idx int) chan Op{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ch, ok := sm.agreeCh[idx]
	if(!ok){
		ch = make(chan Op, 1)
		sm.agreeCh[idx] = ch
	}
	return ch
}

func (sm *ShardMaster) waitAgree(){
	for{
		if sm.killed(){
			break
		} 
		msg := <-sm.applyCh
		op := msg.Command.(Op)
		sm.mu.Lock()
		maxseq, ok := sm.cid_seq[op.Cid]
		if !ok || op.Seq > maxseq{
			newConfig := sm.MakeEmptyConfig()
			sm.CopyConfig(&sm.configs[len(sm.configs)-1], &newConfig)
			newConfig.Num++
			switch op.Op{
			case "Join":
				for k, v := range op.Servers{
					newConfig.Groups[k] = make([]string, len(v))
					for i := 0; i < len(v); i++ {
						newConfig.Groups[k][i] = v[i]
					}
				}
				sm.ReBalance(&newConfig)
				sm.configs = append(sm.configs, newConfig)
			case "Leave":
				for _,g := range op.GIDs{
					delete(newConfig.Groups, g)
					for i:= range newConfig.Shards{
						if newConfig.Shards[i] == g{
							newConfig.Shards[i] = 0
						}
					}
				}
				sm.ReBalance(&newConfig)
				sm.configs = append(sm.configs, newConfig)
			case "Move":
				newConfig.Shards[op.Shard] = op.GID
				sm.configs = append(sm.configs, newConfig)				
			}
		}
		sm.mu.Unlock()
		sm.getAgreeCh(msg.CommandIndex) <- op
	}
}


func (sm *ShardMaster) ReBalance(c *Config) {
	num := len(c.Groups)
	if num == 0{
		return
	}
	avg := NShards / num
	mod := NShards % num

	gid := make([]int, 0)
	cnt := make(map[int]int)
	for g := range c.Groups{
		gid = append(gid,g)
		cnt[g] = 0
	}
	//DPrintf("gid:{%v}", gid)
	for i := range c.Shards{
		if c.Shards[i] == 0{
			c.Shards[i] = gid[0]
			cnt[gid[0]]++
		}else{
			cnt[c.Shards[i]]++
		}
	}
	//DPrintf("cnt:{%v},shards:{%v}", cnt,c.Shards)
	leq := make([]int,0)
	gre := make([]int, 0)
	for _,g := range gid{
		if cnt[g] <= avg{
			leq = append(leq,g)
		}else{
			gre = append(gre,g)
		}
	}

	li := 0
	gi := 0
	for li<len(leq)&&gi<len(gre){
		for li<len(leq)&&cnt[leq[li]] == avg{
			li++
		}
		if li==len(leq){
			break
		}
		cnt[leq[li]]++
		cnt[gre[gi]]--
		for i := range c.Shards{
			if c.Shards[i] == gre[gi]{
				c.Shards[i] = leq[li]
				break
			}
		}
		if cnt[gre[gi]] == avg{
			gi++
		}
	}
	//DPrintf("cnt:{%v},shards:{%v}", cnt,c.Shards)
	gid = append(leq, gre ...)
	li = 0
	ri := len(gid)-1
	for mod>0&&li<ri{
		if cnt[gid[ri]]>avg+1{
			cnt[gid[li]]++
			cnt[gid[ri]]--
			for i := range c.Shards{
				if c.Shards[i] == gid[ri]{
					c.Shards[i] = gid[li]
					break
				}
			}
		}
		li++
		mod++
		if cnt[gid[ri]] == avg+1{
			ri--
			mod++
		}
	}
	//DPrintf("cnt:{%v},shards:{%v}", cnt,c.Shards)
}
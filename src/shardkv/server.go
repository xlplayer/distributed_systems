package shardkv


// import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "sync/atomic"
import "bytes"
import "log"
import "time"
import "../shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//get and pudappend
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	Cid int64
	Seq int32
	Err Err
}

//re-configuration
type Cfg struct {
	NewConfig shardmaster.Config
	Gid int
	Num int
}

//migrate
type Migrate struct {
	Db  map[string]string
	Shards   []int
	Shard_cid_seq [shardmaster.NShards] map[int64] int32
	Gid   int
	Num   int
	Err Err
}

type CommitSend struct {
	Shards   []int
	Gid   int
	Num   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	myshards	 [shardmaster.NShards]int
	curconfig    shardmaster.Config
	newconfig	 shardmaster.Config
	needsend	 [shardmaster.NShards]int
	needrecv	 [shardmaster.NShards]int
	mck	         *shardmaster.Clerk
	persister	 *raft.Persister
	dead    int32 // set by Kill()
	db map[string] string
	shard_cid_seq [shardmaster.NShards] map[int64] int32
	cfg_gid_num map[int] int //re-configuration dup detection
	push_gid_num map[int] int //push dup detection 
	commit_gid_num map[int] int //commit dup dectection
	agreeCh map[int] chan interface{}
}

func (kv *ShardKV) MakeEmptyConfig() shardmaster.Config{
	c := shardmaster.Config{}
	c.Num = 0
	for i := 0; i < shardmaster.NShards; i++ {
		c.Shards[i] = 0
	}
	c.Groups = map[int][]string{}
	return c
}

func (kv *ShardKV) CopyConfig(from *shardmaster.Config, to *shardmaster.Config){
	if to == nil {
		return
	}
	to.Num = from.Num
	for i := 0; i < shardmaster.NShards; i++ {
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

func (kv *ShardKV) encodeSnapshot() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.myshards)	 
	e.Encode(kv.curconfig)
	e.Encode(kv.newconfig)
	e.Encode(kv.needsend)
	e.Encode(kv.needrecv)
	e.Encode(kv.shard_cid_seq)
	e.Encode(kv.db)
	e.Encode(kv.cfg_gid_num)
	e.Encode(kv.push_gid_num)
	e.Encode(kv.commit_gid_num)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte){
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var myshards	 [shardmaster.NShards]int
	var curconfig    shardmaster.Config
	var newconfig	 shardmaster.Config
	var needsend	 [shardmaster.NShards]int
	var needrecv	 [shardmaster.NShards]int 
	var shard_cid_seq [shardmaster.NShards] map[int64] int32
	var db map[string] string
	var cfg_gid_num map[int] int 
	var push_gid_num map[int] int
	var commit_gid_num map[int] int
	if d.Decode(&myshards) != nil ||
		d.Decode(&curconfig) != nil ||
		d.Decode(&newconfig) != nil ||
		d.Decode(&needsend) != nil ||
		d.Decode(&needrecv) != nil ||
		d.Decode(&shard_cid_seq) != nil ||
		d.Decode(&db) != nil ||
		d.Decode(&cfg_gid_num) != nil ||
		d.Decode(&push_gid_num) != nil ||
		d.Decode(&commit_gid_num)!=nil{
	    DPrintf("decode error!")
	} else {
		kv.mu.Lock()
		kv.myshards = myshards
		kv.curconfig = curconfig
		kv.newconfig = newconfig
		kv.needsend = needsend
		kv.needrecv=needrecv
		kv.shard_cid_seq = shard_cid_seq
		kv.db = db
		kv.cfg_gid_num = cfg_gid_num
		kv.push_gid_num = push_gid_num
		kv.commit_gid_num = commit_gid_num
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("group %v-%v start to Get %v",kv.gid,kv.me,args)
	defer DPrintf("group %v-%v get %v",kv.gid,kv.me,reply)
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.myshards[shard] == 0{
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	seq, ok:= kv.shard_cid_seq[key2shard(args.Key)][args.Cid]
	if ok&&seq>=args.Seq{
		reply.Err = OK
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		return
	}
	cmd := Op{"Get",args.Key,"",args.Cid,args.Seq,OK}
	index, _, isLeader := kv.rf.Start(cmd)
	if(!isLeader){
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	ch := kv.getAgreeCh(index)
	select{
	case op := <-ch:
		close(ch)
		if cmd.Cid==op.(Op).Cid&&cmd.Seq==op.(Op).Seq{
			if op.(Op).Err == ErrWrongGroup{
				reply.Err = ErrWrongGroup
			}else{
				kv.mu.Lock()
				var ok bool
				reply.Value,ok = kv.db[args.Key]
				kv.mu.Unlock()
				if !ok{
					reply.Err = ErrNoKey
				}else{
					reply.Err = OK
				}
			}
		}else{
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("group %v-%v start to PutAppend %v",kv.gid,kv.me,args)
	defer DPrintf("group %v-%v PutAppend %v",kv.gid,kv.me,reply)
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.myshards[shard] == 0{
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	seq, ok:= kv.shard_cid_seq[key2shard(args.Key)][args.Cid]
	if ok&&seq>=args.Seq{
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	cmd := Op{args.Op,args.Key,args.Value,args.Cid,args.Seq,OK}
	index, _, isLeader := kv.rf.Start(cmd)
	if(!isLeader){
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	ch := kv.getAgreeCh(index)
	select{
	case op := <-ch:
		close(ch)
		if cmd.Cid==op.(Op).Cid&&cmd.Seq==op.(Op).Seq{
			if op.(Op).Err == ErrWrongGroup{
				reply.Err = ErrWrongGroup
			}else{
				reply.Err = OK
			}
		}else{
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Push(args *PushArgs, reply *PushReply) {
	// Your code here.
	DPrintf("group %v-%v start to Push %v",kv.gid,kv.me,args)
	defer DPrintf("group %v-%v Push %v",kv.gid,kv.me,reply)
	kv.mu.Lock()
	Num, ok:= kv.push_gid_num[args.Gid]
	if ok&&Num>=args.Num{
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	cmd := Migrate{Db:args.Db,Shards:args.Shards,Shard_cid_seq:args.Shard_cid_seq,Gid:args.Gid, Num:args.Num,Err:OK}
	index, _, isLeader := kv.rf.Start(cmd)
	if(!isLeader){
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	ch := kv.getAgreeCh(index)
	select{
	case op := <-ch:
		close(ch)
		if cmd.Gid == op.(Migrate).Gid&&cmd.Num==op.(Migrate).Num{
			if op.(Migrate).Err == ErrNeedWait{
				reply.Err = ErrNeedWait
			}else{
				reply.Err = OK
			}
		}else{
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Cfg{})// without register, receive nil 
	labgob.Register(Migrate{})
	labgob.Register(CommitSend{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.curconfig = kv.MakeEmptyConfig()
	kv.newconfig = kv.MakeEmptyConfig()
	for i:=0;i<shardmaster.NShards;i++{
		kv.needsend[i] = -1
		kv.needrecv[i] = -1
		kv.myshards[i] = 0
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string] string)
	for i:=0;i<shardmaster.NShards;i++{
		kv.shard_cid_seq[i] = make(map[int64] int32)
	}
	kv.cfg_gid_num = make(map[int] int)
	kv.push_gid_num = make(map[int] int)
	kv.commit_gid_num = make(map[int] int)
	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.agreeCh = make(map[int] chan interface{})
	go kv.waitAgree()
	go kv.updateConfig()
	go kv.migrate()

	return kv
}

func (kv *ShardKV) getAgreeCh(idx int) chan interface{}{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.agreeCh[idx]
	if(!ok){
		ch = make(chan interface{}, 1)
		kv.agreeCh[idx] = ch
	}
	return ch
}

func (kv *ShardKV) waitAgree(){
	for{
		if kv.killed(){
			break
		} 
		msg := <-kv.applyCh
		if msg.CommandValid{
			DPrintf("group %v-%v apply %v",kv.gid,kv.me,msg)
		}
		if !msg.CommandValid{
			kv.decodeSnapshot(msg.Command.([]byte))
		}else{
			kv.mu.Lock()
			switch cmd:= msg.Command.(type){
			case Op:
				shard := key2shard(cmd.Key)
				if kv.myshards[shard] == 0{
					cmd.Err = ErrWrongGroup
				}else{
					maxseq, ok := kv.shard_cid_seq[key2shard(cmd.Key)][cmd.Cid]
					if !ok || cmd.Seq > maxseq{
						kv.shard_cid_seq[key2shard(cmd.Key)][cmd.Cid] = cmd.Seq
						if cmd.Op=="Put"{
							kv.db[cmd.Key] = cmd.Value
						}else if cmd.Op=="Append"{
							kv.db[cmd.Key] += cmd.Value
						}
					}
					if kv.maxraftstate!=-1 && kv.persister.RaftStateSize() >= kv.maxraftstate*9/10{
						//DPrintf("currentsize{%v} maxraftstate{%v}",kv.persister.RaftStateSize(), kv.maxraftstate)
						go kv.rf.SaveSnapshot(msg.CommandIndex, kv.encodeSnapshot())
					}
				}
				//DPrintf("group %v-%v finish apply %v",kv.gid,kv.me,msg)
				kv.mu.Unlock()
				kv.getAgreeCh(msg.CommandIndex) <- cmd
				//DPrintf("<<<<<group %v-%v finish apply %v>>>>>",kv.gid,kv.me,msg)

			case Cfg:
				maxNum, ok := kv.cfg_gid_num[cmd.Gid]
				if !ok || cmd.Num > maxNum{
					kv.cfg_gid_num[cmd.Gid] = cmd.Num
					if kv.curconfig.Num == 0{
						for i:=0;i<shardmaster.NShards;i++{
							if cmd.NewConfig.Shards[i] == kv.gid{
								kv.myshards[i] = 1
							}
						}
					}else if cmd.NewConfig.Num == kv.curconfig.Num+1{
						for i:=0;i<shardmaster.NShards;i++{
							if kv.curconfig.Shards[i] == kv.gid && cmd.NewConfig.Shards[i]!=kv.gid{
								kv.needsend[i] = cmd.NewConfig.Shards[i]
								kv.myshards[i] = 0
							}else if kv.curconfig.Shards[i] != kv.gid && cmd.NewConfig.Shards[i]==kv.gid{
								kv.needrecv[i] = kv.curconfig.Shards[i]
								kv.myshards[i] = 0
							}
						}
						DPrintf("group %v-%v needsend:%v,needrecv:%v",kv.gid,kv.me,kv.needsend,kv.needrecv)
					}
					DPrintf("group %v-%v start to updateconfig curconfig:%v,newconfig:%v,needsend:%v,needrecv%v",kv.gid,kv.me,kv.curconfig,cmd.NewConfig,kv.needsend,kv.needrecv)
					kv.newconfig = cmd.NewConfig
					if kv.CheckMigrateDone(){
						kv.curconfig = kv.newconfig
						DPrintf("group %d-%d successful switch to config %d:%v", kv.gid, kv.me, kv.curconfig.Num,kv.curconfig)
					}
				}

				if kv.maxraftstate != -1 {
					go kv.rf.SaveSnapshot(msg.CommandIndex, kv.encodeSnapshot())
				}
				kv.mu.Unlock()
			
			case Migrate:
				if cmd.Num != kv.curconfig.Num {
					cmd.Err = ErrNeedWait
					DPrintf("Migrate: not the same confignum, Group %v-%v curconfig %v, config %d",kv.gid,kv.me, kv.curconfig.Num,cmd.Num)
				}
				for _,shard := range cmd.Shards{
					if kv.needrecv[shard]!=cmd.Gid{
						cmd.Err = ErrNeedWait
						DPrintf("recv error %v from gid %v(need %v)!",shard,cmd.Gid,kv.needrecv[shard])
						break
					}
				}
				if cmd.Err==OK{
					maxNum, ok := kv.push_gid_num[cmd.Gid]
					if !ok || cmd.Num > maxNum{
						kv.push_gid_num[cmd.Gid] = cmd.Num
						for _,shard := range cmd.Shards{
							for k,v := range cmd.Shard_cid_seq[shard]{
								kv.shard_cid_seq[shard][k]=v
							}
						}
						for k,v := range cmd.Db{
							kv.db[k] = v
						}
						for _,shard := range cmd.Shards{
							kv.needrecv[shard] = -1
							kv.myshards[shard] = 1
						}	
						if kv.CheckMigrateDone(){
							kv.curconfig = kv.newconfig
							DPrintf("group %d-%d successful switch to config %d:%v", kv.gid, kv.me, kv.curconfig.Num,kv.curconfig)
						}			
					}
				}

				if kv.maxraftstate != -1 {
					go kv.rf.SaveSnapshot(msg.CommandIndex, kv.encodeSnapshot())
				}
				kv.mu.Unlock()
				kv.getAgreeCh(msg.CommandIndex) <- cmd

			case CommitSend:
				maxNum, ok := kv.commit_gid_num[cmd.Gid]
				if !ok || cmd.Num > maxNum{
					kv.commit_gid_num[cmd.Gid] = cmd.Num
					for _,shard := range cmd.Shards{
						kv.needsend[shard] = -1
						kv.shard_cid_seq[shard] = make(map[int64] int32)
						for k,_ := range kv.db{
							if key2shard(k) == shard{
								delete(kv.db,k)
							}
						}
					}
					DPrintf("group %v-%v finish send to group %v shards:%v ",kv.gid,kv.me,cmd.Gid,cmd.Shards)
					if kv.CheckMigrateDone(){
						kv.curconfig = kv.newconfig
						DPrintf("group %d-%d successful switch to config %d:%v", kv.gid, kv.me, kv.curconfig.Num,kv.curconfig)
					}
				}else{
					DPrintf("group %v-%v CommitSend dup,gid:%v, cmd.Num:%v maxNum:%v",kv.gid,kv.me,cmd.Gid,cmd.Num,maxNum)
				}

				if kv.maxraftstate != -1 {
					go kv.rf.SaveSnapshot(msg.CommandIndex, kv.encodeSnapshot())
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) updateConfig(){
	for{
		if kv.killed(){
			break
		}
		_,isLeader := kv.rf.GetState()
		if !isLeader{
			time.Sleep(100*time.Millisecond)
			continue
		}
		kv.mu.Lock()
		curNum := kv.curconfig.Num
		config := kv.mck.Query(curNum+1)
		if config.Num == curNum+1{
			maxNum, ok := kv.cfg_gid_num[kv.gid]
			if !ok || config.Num>maxNum{
				if kv.CheckMigrateDone(){
					c := kv.MakeEmptyConfig()
					kv.CopyConfig(&config,&c)
					_, _, isLeader := kv.rf.Start(Cfg{c,kv.gid,c.Num})
					if !isLeader{
						time.Sleep(100*time.Millisecond)
						continue
					}
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(100*time.Millisecond)
	}
}

func (kv *ShardKV) migrate(){
	for{
		if kv.killed(){
			break
		}
		_,isLeader := kv.rf.GetState()
		if isLeader{
			kv.mu.Lock()
			gid_shards := make(map[int] []int)
			for shard,gid:= range kv.needsend{
				if gid!= -1{
					_, ok := gid_shards[gid]
					if !ok {
						gid_shards[gid] = make([]int, 0)
					}
					gid_shards[gid] = append(gid_shards[gid],shard)
				}
			}
			DPrintf("group %v-%v gid_shards:%v",kv.gid,kv.me,gid_shards)
			for gid,shards := range gid_shards{
				var shard_cid_seq  [shardmaster.NShards] map[int64] int32
				for i := 0; i < shardmaster.NShards; i++ {
					shard_cid_seq[i] = make(map[int64]int32)
				}
				for _,shard := range shards{
					for k,v := range kv.shard_cid_seq[shard]{
						shard_cid_seq[shard][k]=v
					}
				}
				db := make(map[string]string)
				for k,v := range kv.db{
					shard := key2shard(k)
					for i := range shards{
						if shards[i] == shard{
							db[k] = v
						}
					}
				}
				args := PushArgs{Shards:shards,Shard_cid_seq:shard_cid_seq, Db:db, Gid:kv.gid, Num:kv.curconfig.Num,}
				go func(gid int, args PushArgs){
					servers := kv.newconfig.Groups[gid]
					DPrintf("gid%v,servers:%v",gid,servers)
					for{
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							reply := PushReply{}
							DPrintf("group %v-%v send %v to %v",kv.gid,kv.me,args,servers[si])
							ok := srv.Call("ShardKV.Push", &args, &reply)
							DPrintf("group %v-%v send %v to %v, reply:%v",kv.gid,kv.me,args,servers[si],reply)
							if reply.Err == ErrNeedWait{
								DPrintf("ErrNeedWaits")
								return
							}
							if ok && reply.Err == OK {
								DPrintf("group %v-%v send %v to %v succedds",kv.gid,kv.me,args,servers[si])
								_,isLeader := kv.rf.GetState() 
								if isLeader{
									kv.rf.Start(CommitSend{Shards:args.Shards,Gid:gid,Num:args.Num})//curconfig maybe changed, can't use curconfig.Num
								}
								return
							}
						}
						time.Sleep(100 * time.Millisecond)
					}
					
				}(gid, args)
			} 
			kv.mu.Unlock()
		}
		time.Sleep(100*time.Millisecond)
	}
}

func (kv *ShardKV) CheckMigrateDone() bool {
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.needsend[i] != -1  {
			DPrintf("group %d-%d needsend is not empty", kv.gid, kv.me)
			return false
		}
		if kv.needrecv[i] != -1{
			DPrintf("group %d-%d needrecv is not empty", kv.gid, kv.me)
			return false
		}
	}
	return true
}
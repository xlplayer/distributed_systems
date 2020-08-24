package raft
import "time"

func (rf *Raft)LeaderRun(leaderIdx int){
	go rf.Leadercommit(leaderIdx)
	for idx,_ := range rf.peers{
		if rf.me != idx{
			go func(idx int,leaderIdx int){
				for !rf.killed(){
					rf.mu.Lock()
					if(rf.state != leader || rf.leaderIdx!=leaderIdx){
						rf.mu.Unlock()
						return
					}
					next := rf.nextIndex[idx]
					if(next <= rf.firstIndex){
						go rf.sendInstallSnapshot(idx)
						time.Sleep(10 * time.Millisecond)
						continue
					}
					args := AppendEntriesArgs{
						Term:rf.currentTerm,
						LeaderId:rf.me,
						PreLogIndex:next-1,
						PreLogTerm:rf.getLog(next-1).Term,
						Entries:nil,
						LeaderCommit:rf.commitIndex,
					}
					if next <= rf.lastIdx(){
						args.Entries = append(args.Entries,rf.log[rf.subIdx(next):]...)
					}
					rf.mu.Unlock()

					reply := AppendEntriesReply{}
					c := make(chan bool, 1)
					go func(){
						DPrintf("%v sendAppendEntries to %v:%v",rf.me,idx,args)
						c<-rf.sendAppendEntries(idx,&args,&reply)
					}()
					select {
						case ok:= <- c:
							DPrintf("ok{%v}",ok)
							if !ok{
								time.Sleep(10 * time.Millisecond)
								continue
							}
						case <-time.After(100*time.Millisecond):
							DPrintf("time up")
							continue 
					}
					// DPrintf("%v sendAppendEntries to %v:{%v} Done",rf.me,idx,args)
					rf.mu.Lock()
					if reply.OldTerm != rf.currentTerm || rf.state != leader{
						rf.mu.Unlock()
						return	
					}else if reply.Success && len(args.Entries) > 0{
						rf.matchIndex[idx] = args.PreLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						// DPrintf("%v sendAppendEntries %v success, set rf.nextIndex[%v]=%v, rf.matchIndex[%v]=%v",rf.me,idx,idx,rf.nextIndex[idx],idx,rf.matchIndex[idx])
					}else if reply.Term > rf.currentTerm{
						// DPrintf("{%v} become follower in 489",rf.me)
						rf.state = follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.persist()
						rf.mu.Unlock()
						return
					}else if reply.ConflictIndex > 0{
						conflictIndex := reply.ConflictIndex
						if reply.ConflictTerm != -1{
							for idx,entry := range(rf.log){
								if entry.Term == reply.ConflictTerm{
									for idx<len(rf.log) && rf.log[idx].Term == reply.ConflictTerm{
										idx++
									}
									conflictIndex = rf.addIdx(idx)
									break
								}
							}
						}
						rf.nextIndex[idx]=conflictIndex
					}
					rf.mu.Unlock()
					if len(args.Entries) == 0{
						time.Sleep(100 * time.Millisecond)
					}else{
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(idx,leaderIdx)
		}
	}
}

func (rf *Raft)Leadercommit(leaderIdx int){ 
	for!rf.killed(){
		rf.mu.Lock()
		if(rf.state != leader || leaderIdx != rf.leaderIdx){
			rf.mu.Unlock()
			return
		}
		//DPrintf("%v still learding",rf.me)
		for N := len(rf.log)-1;N>=1&&N>rf.subIdx(rf.commitIndex);N--{
			majority := len(rf.peers)/2+1
			cnt := 1
			if rf.log[N].Term == rf.currentTerm{
				for idx,_ := range rf.matchIndex{
					if rf.matchIndex[idx] >= rf.addIdx(N){
						cnt++
					}
				}
			}
			//DPrintf("N:{%v} majority{%v} cnt{%v}",N,majority,cnt)
			if cnt>=majority{
				rf.commitIndex = rf.addIdx(N)
				rf.applyCond.Broadcast()
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(10*time.Millisecond)
	}
}

func (rf *Raft)sendInstallSnapshot(server int){
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.firstIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:       	   rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	c := make(chan bool, 1)
	go func(){
		DPrintf("{%v} send installsnapshot to {%v}",rf.me,server)
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		c <- ok
		DPrintf("{%v} send installsnapshot to {%v} finish",rf.me,server)
	}()
	select {
		case ok:= <- c:
			DPrintf("ok{%v}",ok)
			if !ok{
				return
			}
		case <-time.After(100*time.Millisecond):
			DPrintf("time up")
			return 
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.OldTerm != rf.currentTerm || rf.state != leader{
		return
	}else if reply.Term > rf.currentTerm{
		rf.state = follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.persist()
	}else{
		rf.matchIndex[server] = args.LastIncludedIndex
    	rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}
package raft
import "time"
import "sync"

func (rf *Raft)FollowerRun(){
	for !rf.killed(){
		//DPrintf("{%v}:want to lock",rf.me)
		rf.mu.Lock()
		//DPrintf("{%v}:finished to lock",rf.me)
		if(rf.state != follower){
			rf.mu.Unlock()
			time.Sleep(10*time.Millisecond)
			rf.mu.Lock()
			rf.timer.last_time = time.Now()
			rf.mu.Unlock()
			continue
		}
		//DPrintf("{%v}:%v %v",rf.me, time.Now(), rf.timer.last_time)
		if time.Now().After(rf.timer.last_time.Add(rf.timer.election_timeout)){
			DPrintf("%v start to election",rf.me)
			rf.state = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()

			args := RequestVoteArgs{
				Term:rf.currentTerm,
				CandidateId:rf.me,
				LastLogIndex:rf.lastIdx(),
				LastLogTerm:rf.lastTerm(),
			}
			rf.mu.Unlock()

			replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
			var wg sync.WaitGroup
			for idx,_ := range rf.peers{
				if rf.me != idx{
					wg.Add(1)
					go func(idx int){
						defer wg.Done()
						reply := RequestVoteReply{}
						c := make(chan bool, 1)
						go func(){
							DPrintf("%v sendRequestVote to %v",rf.me,idx)
							c<-rf.sendRequestVote(idx,&args,&reply)
						}()
						select {
							case ok := <-c:
								if ok{
									replyCh <- reply
								}
							case <-time.After(100*time.Millisecond):
								return 
						}
					}(idx)
				}
			}
			go func() {
				wg.Wait()
				close(replyCh)
			}()

			rf.mu.Lock()
			votes := 1
			majority := len(rf.peers)/2 + 1
			for reply := range replyCh{
				if reply.OldTerm != rf.currentTerm{
					continue
				}
				if reply.Term > rf.currentTerm{
					DPrintf("{%v} become follower in 413",rf.me)
					rf.state = follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()
					break
				}else if reply.VoteGrantd{
					votes++
				}
				if votes >= majority{
					DPrintf("%v win the election",rf.me)
					//rf.votedFor = -1 //a leader should not vote
					for idx,_ := range rf.peers{
						rf.nextIndex[idx] = rf.lastIdx()+1
						rf.matchIndex[idx] = 0
					}
					rf.state = leader
					go rf.LeaderRun()
					break
				}

			}
			if rf.state == leader{
				rf.mu.Unlock()
				continue
			}
			DPrintf("%v loss the election",rf.me)
			rf.timer.last_time = time.Now()
			rf.state = follower//vote failure, don't get enough votes
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			// rf.mu.Lock()
			// rf.timer.last_time = time.Now()
			// rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	DPrintf("{%v} received installsnapshot from {%v}",rf.me,args.LeaderId)
	rf.mu.Lock()
    defer rf.mu.Unlock()
	reply.OldTerm = args.Term
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
        return
	}
	if args.Term > rf.currentTerm{
		rf.state = follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.timer.last_time = time.Now()
	if args.LastIncludedIndex <= rf.firstIndex{
		return
	}

	if args.LastIncludedIndex <= rf.lastIdx(){
		rf.log = rf.log[rf.subIdx(args.LastIncludedIndex):]
	}else{
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	rf.firstIndex = args.LastIncludedIndex
	rf.persistStatesAndSnapshot(args.Data)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.applyCh <- ApplyMsg{
        CommandValid: false,
        CommandIndex: -1,
        Command: args.Data,
	}
}
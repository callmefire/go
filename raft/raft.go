package raft

import (
    "time"
    "encoding/json"
    "sync"
    "math/rand"
    "errors"
    "fmt"
)

const (
    RAFT_STATE_UNKNOWN   = 0
    RAFT_STATE_FOLLOWER  = 1
    RAFT_STATE_CANDIDATE = 2
    RAFT_STATE_LEADER    = 3

    PEER_ALIVE_LIFECYCLE = 5
)

type RaftState struct {
    State   uint16
    Op      uint16
    /* local address */
    Addr    uint32
    /* Node address to be voted */
    VAddr   uint32
}

type RaftIO interface {
    Send([]byte, uint16) error
}

type RaftPeer struct {
    lifecycle   uint8
    vote        uint8
}

/* Control block */
type RaftCtrlBlock struct {
    state       RaftState
    /* Mutex to protect state */
    smutex      sync.Mutex
    /* timer for state machine */
    stateTimer  *time.Timer
    sendTimer   *time.Ticker
    scanTimer   *time.Ticker
    peerlist    map[uint32] *RaftPeer
    /* Mutex to protect peer list */
    lmutex      sync.Mutex
    leader      uint32
    candidate   uint32
    io          RaftIO
    debug       bool
}

var (
    EEXIST = errors.New("Component exists")
    ENOENT = errors.New("Component doesn't exist")
    ENOMEM = errors.New("No memory")
)

func (rcb *RaftCtrlBlock) Debug(args ... interface{}) {
    if rcb.debug == false {
        return
    }
    fmt.Print(args,"\n")
}

func (rcb *RaftCtrlBlock) SetDebug(val bool) {
    rcb.debug = val
}

/* Scan the events to be handled */
func (rcb *RaftCtrlBlock) scaner() {
    for {
        rcb.Debug("[", rcb.state.State,"]:", "scan", rcb.peerlist)
        select {
        case <-rcb.sendTimer.C:
            rcb.Debug("[", rcb.state.State, "]:", "Send timer triigered")
            rcb.send()
        case <-rcb.scanTimer.C:
            rcb.Debug("[", rcb.state.State, "]:", "Age timer triigered")
            rcb.age()
        case <-rcb.stateTimer.C:
            rcb.Debug("[", rcb.state.State, "]:", "State timer triigered")
            if rcb.state.State == RAFT_STATE_FOLLOWER {
                if rcb.candidate == 0 && rcb.leader == 0 {
                    rcb.Candidate()
                }
            } else if rcb.state.State == RAFT_STATE_CANDIDATE {
                rcb.Debug("[", rcb.state.State, "]:","ARB: ",rcb.countvote(), len(rcb.peerlist))
                if rcb.countvote() > uint32(len(rcb.peerlist)/2) || len(rcb.peerlist) == 1 {
                    rcb.Leader()
                } else {
                    rcb.Follower()
                }
            }
        }
    }
}

func (rcb *RaftCtrlBlock) age() {
    rcb.lmutex.Lock()
    for key := range rcb.peerlist {
        if key == rcb.state.Addr {
            continue
        }
        rcb.peerlist[key].lifecycle--
        if rcb.peerlist[key].lifecycle == 0 {
            rcb.remove(key)
            if key == rcb.leader || key == rcb.candidate {
                rcb.Follower()
            }
        }
    }
    rcb.lmutex.Unlock()
}

func (rcb *RaftCtrlBlock) refresh(idx uint32) {
    rcb.lmutex.Lock()
    rcb.peerlist[idx].lifecycle = PEER_ALIVE_LIFECYCLE
    rcb.lmutex.Unlock()
}

func (rcb *RaftCtrlBlock) remove(idx uint32) error {
    _, ok := rcb.peerlist[idx]
    if ok == false {
        return ENOENT
    }
    delete(rcb.peerlist, idx)
    return nil
}

func (rcb *RaftCtrlBlock) add(idx uint32) error {
    _, ok := rcb.peerlist[idx]
    if ok == true {
        return EEXIST
    }
    peer := new(RaftPeer)
    if peer == nil {
        return ENOMEM
    }
    peer.lifecycle = PEER_ALIVE_LIFECYCLE
    peer.vote = 0

    rcb.lmutex.Lock()
    rcb.peerlist[idx] = peer
    rcb.lmutex.Unlock()
    return nil
}

func (rcb *RaftCtrlBlock) setvote(idx uint32,val uint8) {
    rcb.lmutex.Lock()
    rcb.peerlist[idx].vote = val
    rcb.lmutex.Unlock()
}

func (rcb *RaftCtrlBlock) clearvote() {
    for key := range rcb.peerlist {
        if key != rcb.state.Addr {
            rcb.setvote(key, 0)
        }
    }
}

func (rcb *RaftCtrlBlock) countvote() uint32 {
    var cnt uint32
    rcb.lmutex.Lock()
    for key := range rcb.peerlist {
        if rcb.peerlist[key].vote > 0 {
            cnt++
        }
    }
    rcb.lmutex.Unlock()
    return cnt
}

/* Exported API, can be used bas callback and embedded in IO.Receive()  */
func (rcb *RaftCtrlBlock) Parser(data []byte, dlen uint16)  error {
    remote := new(RaftState)

    if err := json.Unmarshal(data[:dlen], remote); err != nil {
        return err
    }

    if _, ok := rcb.peerlist[remote.Addr]; ok == false {
        rcb.add(remote.Addr)
    } else {
        rcb.refresh(remote.Addr)
    }

    switch rcb.state.State {
        case RAFT_STATE_FOLLOWER:
            if remote.State == RAFT_STATE_CANDIDATE {
                if remote.Addr > rcb.candidate {
                    rcb.candidate = remote.Addr
                }
                /* Candidate has been there */
                rcb.stateTimer.Stop()
            } else if remote.State == RAFT_STATE_LEADER {
                rcb.leader = remote.Addr
            }

            break
        case RAFT_STATE_CANDIDATE:
            if remote.State == RAFT_STATE_CANDIDATE {
                if rcb.state.Addr < remote.Addr {
                    rcb.Follower()
                }
            } else if remote.State == RAFT_STATE_FOLLOWER {
                if remote.VAddr == rcb.state.Addr {
                    rcb.setvote(remote.Addr, 1)
                }
            } else {
                rcb.Follower()
            }
        case RAFT_STATE_LEADER:
            if remote.State == RAFT_STATE_LEADER {
                rcb.Follower()
            }
            break
        default:
            break
    }

    return nil
}

func (rcb *RaftCtrlBlock) vote() {
    rcb.state.VAddr = rcb.candidate
    rcb.Debug("[", rcb.state.State, "]:", "vote for", rcb.candidate)
}

func (rcb *RaftCtrlBlock) send() error {
    var data []byte
    var err error

    rcb.Debug("[", rcb.state.State, "]:", "send")
    rcb.smutex.Lock()
    defer rcb.smutex.Unlock()
    switch rcb.state.State {
        case RAFT_STATE_FOLLOWER:
            rcb.vote()
            data, err = rcb.BuildMsg()
        case RAFT_STATE_CANDIDATE, RAFT_STATE_LEADER:
            rcb.Debug("[", rcb.state.State, "]: build msg")
            data, err = rcb.BuildMsg()
        default:
            return nil
    }
    if err == nil {
        return rcb.io.Send(data, (uint16)(len(data)))
    } else {
        rcb.Debug("[", rcb.state.State, "]: send err ", err)
        return err
    }
}

func (rcb *RaftCtrlBlock) Init(addr uint32, io RaftIO, async bool) error {
    rcb.state.State = RAFT_STATE_FOLLOWER
    rcb.state.Addr  = addr
    rcb.state.VAddr   = 0
    rcb.io    = io

    rcb.Debug("[", rcb.state.State, "]: addr ", addr)

    rcb.peerlist = make(map[uint32] *RaftPeer)
    if rcb.peerlist == nil {
        return ENOMEM
    }

    /* Vote for myself */
    rcb.add(addr)
    rcb.setvote(addr, 1)

    rcb.Follower()

    rcb.sendTimer = time.NewTicker(1000*time.Millisecond)
    rcb.scanTimer = time.NewTicker(1000*time.Millisecond)

    if async {
        go rcb.scaner()
    } else {
        rcb.scaner()
    }

    return nil
}

func (rcb *RaftCtrlBlock) Stop() {

}


func (rcb *RaftCtrlBlock) BuildMsg() ([]byte, error) {
    rcb.Debug("[",rcb.state.State,"]: content", rcb.state)
    data, err := json.Marshal(rcb.state)
    return data, err
}

func (rcb *RaftCtrlBlock) Follower() {
    rcb.smutex.Lock()
    rcb.state.State = RAFT_STATE_FOLLOWER
    rcb.candidate   = 0
    rcb.leader      = 0

    /* Start random timer to be Candidate */
    rand.Seed(time.Now().UnixNano())
    interval := time.Duration(rand.Intn(7) * 1000 + 3000) * time.Millisecond
    rcb.Debug("[", rcb.state.State, "]:", "set timeout", interval)
    if rcb.stateTimer == nil {
        rcb.stateTimer = time.NewTimer(interval)
    } else {
        rcb.stateTimer.Reset(interval)
    }
    rcb.smutex.Unlock()
}

func (rcb *RaftCtrlBlock) Candidate() {
    rcb.smutex.Lock()
    rcb.state.State = RAFT_STATE_CANDIDATE
    rcb.candidate   = rcb.state.Addr
    rcb.leader      = 0

    rcb.Debug("[", rcb.state.State, "]:")
    /* Start ARB window timer */
    if rcb.stateTimer == nil {
        rcb.stateTimer = time.NewTimer(3000*time.Millisecond)
    } else {
        rcb.stateTimer.Reset(3000*time.Millisecond)
    }
    rcb.smutex.Unlock()
    rcb.clearvote()
}

func (rcb *RaftCtrlBlock) Leader() {
    rcb.smutex.Lock()
    rcb.state.State = RAFT_STATE_LEADER
    rcb.candidate   = 0
    rcb.leader      = rcb.state.Addr
    rcb.smutex.Unlock()
    rcb.Debug("[", rcb.state.State, "]:")
}

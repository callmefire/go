package main

import (
    "raft"
    "mcastrpc"
    "flag"
)

const (
    RAFTCLUSTER = 0x88
)

type Cluster struct {
    rpc     *mcastrpc.McastRPC
    raft    *raft.RaftCtrlBlock
}

func main() {

    id := flag.Uint("id", 0, "id")
    flag.Parse()

    cluster := new(Cluster)
    cluster.rpc = new(mcastrpc.McastRPC)

    cluster.rpc.RegisterReqHandler(RAFTCLUSTER, cluster.Receive, nil)
    cluster.rpc.Start("234.5.6.7:9000")

    cluster.raft = new(raft.RaftCtrlBlock)
    cluster.raft.Init(uint32(*id), cluster, false);

}

func (cluster *Cluster) Receive(data []byte, dlen uint16 ) error {
    return cluster.raft.Parser(data, dlen)
}

func (cluster *Cluster) Send(data []byte, dlen uint16) error {
    return cluster.rpc.Send(RAFTCLUSTER, true, data)
}
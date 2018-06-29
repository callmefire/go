package mcastrpc

import (
    "testing"
)

const (
    mcast_group_addr =  "234.5.6.7:9000"
)

func mcastrpc_alloc() *McastRPC {
    rpc := new(McastRPC)
    return rpc
}

func Test_McastRPC(t *testing.T) {
    rpc := mcastrpc_alloc()
    if rpc == nil {
        return
    }

    rpc.Start(mcast_group_addr)
}

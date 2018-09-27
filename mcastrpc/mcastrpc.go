package mcastrpc

import (
    "net"
    "sync"
    "errors"
    "encoding/json"
)

const (
    DefaultDgramSize  = 8192
)

/*
Message format 
---------------------------------------------
|   8bit   |   8bit   |   8bit   |   8bit   |
|--------------------------------------------
|                    op                     |
|-------------------------------------------|
|                   seqno                   |
|-------------------------------------------|
|        flag         |        size         |
|-------------------------------------------|
|                   data                    |
|-------------------------------------------|

Json format
{
    "op"   : "1", 
    "seqno": "1",
    "flag" : "1",
    "size" : "64"
    "data" : [ ]
}
*/

/* Error definitions */
var EEXIST = errors.New("Component exists")
var ENOENT = errors.New("Component doesn't exist")
var EPTR   = errors.New("Pointer is nil")
var ENOMEM = errors.New("Memory allocation failed")

type McastMsgHeader struct {
    OP    uint32    `json:"op"`
    Seq   uint32    `json:"seq"`
    Flag  uint16    `json:"flag"`
    Dlen  uint16    `json:"dlen"`
    Data  []byte    `json:"data"`
}

type McastReq struct {
    reqfunc    func ([]byte, uint16) error
}

/* MRPC context */
type McastRPC struct {
    conn         *net.UDPConn
    /* Mcast group address and port */
    addr         *net.UDPAddr
    max_dgram_size int
    /* Request mapping */
    reqs         map[uint32] *McastReq

    /* Sequence number */
    seq          uint32

    closed       bool
    wait         sync.WaitGroup
}

func (rpc *McastRPC) RegisterReqHandler (op uint32, reqhandler func([]byte, uint16) error) error {

    if rpc.reqs == nil {
        rpc.reqs = make(map[uint32] *McastReq)
    }

    _, ok := rpc.reqs[op]
    if ok == true {
        return EEXIST
    }
    req := new(McastReq)
    req.reqfunc  = reqhandler
    rpc.reqs[op] = req

    return nil
}

func (rpc *McastRPC) SetMaxDgramSize(size int) {
    rpc.max_dgram_size = size
}

func (rpc *McastRPC) GetMaxDgramSize() int {
    return rpc.max_dgram_size
}

func (rpc *McastRPC) listener() {
    defer rpc.wait.Done()

    for rpc.closed == false {
        rpc.receive()
    }
}

/* MRPC initialization */
func (rpc *McastRPC) Start(gaddr string) error {
    var err error
    rpc.addr, err = net.ResolveUDPAddr("udp", gaddr)
    if err != nil {
        return err
    }

    rpc.conn, err = net.ListenMulticastUDP("udp", nil, rpc.addr)
    if err != nil {
        return err
    }

    if (rpc.reqs == nil) {
        rpc.reqs = make(map[uint32] *McastReq)
    }

    if rpc.max_dgram_size == 0 {
        rpc.max_dgram_size = DefaultDgramSize
    }

    rpc.seq = 0

    rpc.closed = false
    rpc.wait.Add(1)

    go rpc.listener()

    return err
}

func (rpc *McastRPC) Stop()  {

    /* Tell rpc.listener to quit */
    rpc.closed = true
    /* Wait rpc.listener to quit */
    rpc.wait.Wait()

    rpc.conn.Close()
}

func (rpc *McastRPC) Send(op uint32, data []byte) error {
    header := new(McastMsgHeader)
    header.OP   = op
    header.Seq  = rpc.seq
    header.Dlen = (uint16)(len(data))

    header.Data = append(header.Data, data...)
    rpc.seq++


    rawdata, err := json.Marshal(*header)
    if err != nil {
        return err
    }
    rpc.wait.Add(1)

    /* I don't understand why WriteToUDP works but Write not. Seems like a bug */
    //rpc.conn.Write(rawdata)
    rpc.conn.WriteToUDP(rawdata, rpc.addr)
    rpc.wait.Done()

    return  err
}

func (rpc *McastRPC) receive() error {

    var header *McastMsgHeader
    var err error

    defer rpc.wait.Done()
    rpc.wait.Add(1)

    buf := make([]byte, rpc.max_dgram_size)
    n, _ , err := rpc.conn.ReadFromUDP(buf)
    if err != nil {
        return err
    }

    if header, err = rpc.parse(buf, n); err != nil {
        return err
    }

    if _, ok := rpc.reqs[header.OP]; ok == false {
        return ENOENT
    }

    if reqfunc := rpc.reqs[header.OP].reqfunc; reqfunc != nil {
        return reqfunc(header.Data, header.Dlen)
    }

    return nil
}

func (rpc *McastRPC) parse(data []byte, len int) (*McastMsgHeader, error) {

    var err error

    header := new(McastMsgHeader)
    if header == nil {
        return nil, ENOMEM
    }

    if err = json.Unmarshal(data[:len], header); err != nil {
        return nil, err
    }

    return header, err
}

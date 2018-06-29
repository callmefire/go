package main

import (
    "mcastrpc"
    "fmt"
    "os"
    "time"
    "encoding/json"
)

const (
    SENDHOSTNAME = 0x88
)


type mrpcreq struct {
    Magic1 uint32
    Name   []byte
    Magic2 uint32
}

func create_req() *mrpcreq {
    if req := new(mrpcreq); req == nil {
        return nil
    }

    req := new(mrpcreq)
    req.Magic1 = 12345678
    req.Magic2 = 87654321

    name, err := os.Hostname()
    if err != nil {
        return nil
    }

    req.Name = append(req.Name, []byte(name)...)
    return req
}

func main() {
    rpc := new(mcastrpc.McastRPC)

    rpc.RegisterReqHandler(SENDHOSTNAME, getreq, getresp)
    rpc.Start("234.5.6.7:9000")

    req := create_req()
    if req == nil {
        return
    }
    data, err := json.Marshal(req)
    if err != nil {
        return
    }

    time.Sleep(5000*time.Millisecond)
    for {
        rpc.Send(SENDHOSTNAME, true, data)
        time.Sleep(1000*time.Millisecond)
    }

}

func getreq(rpc *mcastrpc.McastRPC, data []byte, dlen uint16 ) error {

    req := new(mrpcreq)
    if err := json.Unmarshal(data[:dlen],req); err != nil {
        return err
    }
    fmt.Println("Recv req from ",string(req.Name), req.Magic1, req.Magic2)
/*
    resp := create_req()
    if resp == nil {
        return nil
    }

    newdata, err := json.Marshal(*resp)
    if err != nil {
        return err
    }
    rpc.Send(SENDHOSTNAME, false, newdata)
*/
    return nil
}

func getresp(rpc *mcastrpc.McastRPC, data []byte, dlen uint16 ) error {
    req := new(mrpcreq)
    if err := json.Unmarshal(data[:dlen],req); err != nil {
        return err
    }
    fmt.Println("Recv resp from ",string(req.Name), req.Magic1, req.Magic2)

    return nil
}

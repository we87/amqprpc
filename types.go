package amqprpc

import (
	"encoding/json"
	"fmt"
	"time"
)

type Request struct {
	Func    string                 `json:"func"`
	Payload map[string]interface{} `json:"payload"`
}

type Response struct {
	Code   int         `json:"code"`
	Msg    string      `json:"msg"`
	Result interface{} `json:"result"`
}

type Future struct {
	value chan []byte
}

func NewFuture() *Future {
	v := make(chan []byte)
	return &Future{
		value: v,
	}
}

func (fu *Future) Get(timeout int) Response {
	var rsp Response
	select {
	case v := <-fu.value:
		err := json.Unmarshal(v, &rsp)
		if err != nil {
			rsp = Response{
				Code:   500,
				Msg:    fmt.Sprintf("invoke load error %s", err),
				Result: nil,
			}
		}
	case <-time.After(time.Duration(timeout) * time.Second):
		rsp = Response{
			Code:   500,
			Msg:    fmt.Sprintf("invoke timeout"),
			Result: nil,
		}
	}
	return rsp
}

func (fu *Future) Put(v []byte) {
	fu.value <- v
}

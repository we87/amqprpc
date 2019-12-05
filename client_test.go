package amqprpc

import (
	"fmt"
	"testing"
	"time"
)

func TestInvoke(t *testing.T) {
	client, err := NewClient("hello", "amqp://guest:guest@localhost:5672")
	if err != nil {
		t.Error(err)
	}
	payload := map[string]interface{}{
		"args1": "this is a rpc request",
		"args2": 123,
		"args4": 5.5,
		"args5": true,
	}
	for i := 1; i <= 10; i++ {
		req := Request{
			Func:    "boo",
			Payload: payload,
		}
		rsp := client.Invoke(req, 1)
		fmt.Println(rsp)
		time.Sleep(1 * time.Second)
	}
}

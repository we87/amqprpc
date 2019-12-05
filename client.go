package amqprpc

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"k8s.io/klog"
	"sync"
	"time"
)

type Client struct {
	connURL         string
	conn            *amqp.Connection
	rpcQueue        string
	channel         *amqp.Channel
	callbackConn    *amqp.Connection
	callbackQueue   string
	callbackChannel *amqp.Channel
	cu              *sync.Mutex
	invokeMap       map[string]*Future
	mu              *sync.Mutex
}

func NewClient(rpcQueue string, connURL string) (*Client, error) {
	c := &Client{
		connURL:         connURL,
		conn:            nil,
		rpcQueue:        rpcQueue,
		channel:         nil,
		callbackConn:    nil,
		callbackChannel: nil,
		callbackQueue:   "",
		cu:              new(sync.Mutex),
		invokeMap:       make(map[string]*Future),
		mu:              new(sync.Mutex),
	}
	err := c.start()
	return c, err
}

func (c *Client) Invoke(req Request, timeout int) Response {
	var rsp = Response{
		Code:   500,
		Result: nil,
	}
	corrUUID, err := uuid.NewUUID()
	if err != nil {
		rsp.Msg = "invoke error 0"
		return rsp
	}
	//generate future
	corrID := corrUUID.String()
	fu := NewFuture()
	c.mu.Lock()
	c.invokeMap[corrID] = fu
	c.mu.Unlock()
	//callback queue is ok?
	c.cu.Lock()
	var callbackOk bool
	callbackOk = (c.callbackQueue != "")
	c.cu.Unlock()
	if !callbackOk {
		rsp.Msg = "invoke error 1"
		return rsp
	}
	//publish
	ch, err := c.getChannel()
	if err != nil {
		rsp.Msg = "invoke error 2"
		return rsp
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		rsp.Msg = "invoke error 3"
		return rsp
	}
	err = ch.Publish("", c.rpcQueue, false, false, amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: corrID,
		ReplyTo:       c.callbackQueue,
		Body:          reqBytes,
	})
	if err != nil {
		rsp.Msg = "invoke error 4"
		return rsp
	}
	//result
	rsp = fu.Get(timeout)
	c.mu.Lock()
	delete(c.invokeMap, corrID)
	c.mu.Unlock()
	return rsp
}

func (c *Client) getConnect() (*amqp.Connection, error) {
	if c.conn == nil || c.conn.IsClosed() {
		conn, err := amqp.Dial(c.connURL)
		c.conn = conn
		return c.conn, err
	}
	return c.conn, nil
}

func (c *Client) getChannel() (*amqp.Channel, error) {
	conn, err := c.getConnect()
	if err != nil {
		return nil, err
	}
	if c.channel == nil {
		ch, err := conn.Channel()
		c.channel = ch
		return c.channel, err
	}
	return c.channel, nil
}

func (c *Client) getCallbackConnect() (*amqp.Connection, error) {
	if c.callbackConn == nil || c.callbackConn.IsClosed() {
		conn, err := amqp.Dial(c.connURL)
		c.callbackConn = conn
		return c.callbackConn, err
	}
	return c.callbackConn, nil
}

func (c *Client) getCallbackChannel() (*amqp.Channel, error) {
	conn, err := c.getCallbackConnect()
	if err != nil {
		return nil, err
	}
	if c.callbackChannel == nil {
		ch, err := conn.Channel()
		c.callbackChannel = ch
		return c.callbackChannel, err
	}
	return c.callbackChannel, nil
}

//建立callback连接并声明callback队列
func (c *Client) handleCallback() *amqp.Channel {
	for {
		callbachCh, err := c.getCallbackChannel()
		if err != nil {
			klog.Error("rpc client callback get channel error")
			klog.Error(err)
			c.callbackConn.Close()
			c.callbackConn = nil
			time.Sleep(500 * time.Microsecond)
			continue
		}
		queue, err := callbachCh.QueueDeclare("", false, false, true, false, nil)
		if err != nil {
			klog.Error("rpc client callback declare queue error")
			klog.Error(err)
			c.callbackConn.Close()
			c.callbackConn = nil
			time.Sleep(500 * time.Microsecond)
			continue
		}
		c.cu.Lock()
		c.callbackQueue = queue.Name
		c.cu.Unlock()
		return callbachCh
	}
}

//清除callback队列并断开连接
func (c *Client) initCallback() {
	c.cu.Lock()
	c.callbackQueue = ""
	c.cu.Unlock()
	if err := c.callbackConn.Close(); err != nil {
		klog.Error("rpc client callback connect close error")
		klog.Error(err)
		c.callbackConn = nil
	}
}

//启动callback
func (c *Client) start() error {
	//启动的时候就准备好callback queue
	callbachCh := c.handleCallback()
	//设置响应
	go func(c *Client) {
		for {
			if c.callbackQueue == "" {
				callbachCh = c.handleCallback()
			}
			responses, err := callbachCh.Consume(c.callbackQueue, "", false, false, false, false, nil)
			if err != nil {
				klog.Error("rpc client callback consume error")
				klog.Error(err)
				c.initCallback()
				continue
			}
			klog.Info("rpc client ready invoke")
			for rsp := range responses {
				c.mu.Lock()
				fu, ok := c.invokeMap[rsp.CorrelationId]
				if ok {
					fu.Put(rsp.Body)
				}
				c.mu.Unlock()
				if rsp.Ack(false) != nil {
					//消息Ack发送失败则认为连接出错
					break
				}
			}
			//关闭当前callback连接重开
			c.initCallback()
		}
	}(c)
	return nil
}

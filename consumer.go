package redis_mq

import (
	"context"
	"fmt"
	"redis-mq/redis"
	"time"
)

const (
	defaultReceiveMsgTimeout = 2 * time.Second
	defaultMaxRetryLimit     = 2
	defaultHandleMsgTimeout  = 2 * time.Second
	defaultDeliverMsgTimeout = 2 * time.Second
)

// MsgCallback 接收到消息后的回调函数
type MsgCallback func(ctx context.Context, msg *redis.Msg) error

type Consumer struct {
	client            *redis.Client
	ctx               context.Context
	cancel            context.CancelFunc
	topic             string
	group             string
	consumer          string
	failureCnts       map[*redis.Msg]int
	maxRetryLimit     int           // 最大重试次数
	callbackFunc      MsgCallback   // 消息处理回调函数
	deadLetter        DeadLetter    // 死信队列
	receiveMsgTimeout time.Duration // 接收消息超时时长
	handleMsgTimeout  time.Duration // 处理消息超时时长
	deliverMsgTimeout time.Duration // 发送死信队列超时时长
}

func NewConsumer(client *redis.Client, topic, group, consumer string, callbackFunc MsgCallback, deadLetter DeadLetter) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		client:            client,
		ctx:               ctx,
		cancel:            cancel,
		callbackFunc:      callbackFunc,
		topic:             topic,
		group:             group,
		consumer:          consumer,
		failureCnts:       make(map[*redis.Msg]int),
		maxRetryLimit:     defaultMaxRetryLimit,
		receiveMsgTimeout: defaultReceiveMsgTimeout,
		handleMsgTimeout:  defaultHandleMsgTimeout,
		deliverMsgTimeout: defaultDeliverMsgTimeout,
		deadLetter:        deadLetter,
	}
	go c.run()
	return c
}

func (c *Consumer) run() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// 获取已经接收的消息
		msgs, err := c.receive("0-0")
		if err != nil {
			fmt.Printf("receive msg failed, err: %v", err)
			continue
		}

		tctx, _ := context.WithTimeout(c.ctx, c.handleMsgTimeout)
		c.handle(tctx, msgs)

		// 死信队列投递
		tctx, _ = context.WithTimeout(c.ctx, c.deliverMsgTimeout)
		c.deliver(tctx)

		// 阻塞接收新消息
		pendingMsgs, err := c.receive(">")
		if err != nil {
			fmt.Printf("pending msg received failed, err: %v", err)
			continue
		}

		tctx, _ = context.WithTimeout(c.ctx, c.handleMsgTimeout)
		c.handle(tctx, pendingMsgs)
	}
}

func (c *Consumer) receive(typ string) ([]*redis.Msg, error) {
	msgs, err := c.client.XREADGROUP(c.ctx, c.group, c.consumer, c.topic, 0, typ)
	return msgs, err
}

func (c *Consumer) handle(ctx context.Context, msgs []*redis.Msg) {
	for _, msg := range msgs {
		// 通过回调函数执行具体业务
		if err := c.callbackFunc(ctx, msg); err != nil {
			// 失败计数器累加
			c.failureCnts[msg]++
			continue
		}

		// callback 执行成功，进行 ack
		if err := c.client.XACK(ctx, c.topic, c.group, msg.MsgId); err != nil {
			fmt.Printf("msg ack failed, msg id: %s, err: %v", msg.MsgId, err)
			continue
		}

		delete(c.failureCnts, msg)
	}
}

func (c *Consumer) deliver(ctx context.Context) {
	// 对于失败达到指定次数的消息，投递到死信中，然后执行 ack
	for msg, failureCnt := range c.failureCnts {
		if failureCnt < c.maxRetryLimit {
			continue
		}

		// 投递死信队列
		if err := c.deadLetter.Deliver(ctx, msg); err != nil {
			fmt.Printf("dead letter deliver failed, msg id: %s, err: %v", msg.MsgId, err)
		}

		// 执行 ack 响应
		if err := c.client.XACK(ctx, c.topic, c.group, msg.MsgId); err != nil {
			fmt.Printf("msg ack failed, msg id: %s, err: %v", msg.MsgId, err)
			continue
		}

		// 对于 ack 成功的消息，将其从 failure map 中删除
		delete(c.failureCnts, msg)
	}
}

func (c *Consumer) Stop() {
	c.cancel()
}

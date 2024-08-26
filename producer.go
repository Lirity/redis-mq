package redis_mq

import (
	"context"
	"fmt"
	"redis-mq/redis"
	"time"
)

type Producer struct {
	rdb    *redis.Client
	ctx    context.Context // 控制整个生产者链路
	cancel context.CancelFunc
	topic  string
	maxLen int64
}

func NewProducer(client *redis.Client, topic string, maxLen int64) *Producer {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		rdb:    client,
		ctx:    ctx,
		cancel: cancel,
		topic:  topic,
		maxLen: maxLen,
	}
	go p.run()
	return p
}

func (p *Producer) run() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		// 每隔1s记录一次系统时间
		time.Sleep(1 * time.Second)
		msgId, err := p.send(p.ctx, p.topic, p.maxLen, map[string]string{"time": time.Now().String()})
		if err != nil {
			p.Stop()
			fmt.Printf("send msg err: %v", err)
		}
		fmt.Printf("send msg success: %s", msgId)
	}
}

func (p *Producer) Stop() {
	p.cancel()
}

func (p *Producer) send(ctx context.Context, topic string, maxLen int64, message map[string]string) (string, error) {
	return p.rdb.XADD(ctx, topic, maxLen, message)
}

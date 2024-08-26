package redis_mq

import (
	"context"
	"fmt"
	"redis-mq/redis"
)

// DeadLetter 当消息处理失败到达指定次数时，加入死信队列
type DeadLetter interface {
	Deliver(ctx context.Context, msg *redis.Msg) error
}

// DeadLetterLogger 日志死信队列，记录加入死信队列的消息
type DeadLetterLogger struct{}

func NewDeadLetterLogger() *DeadLetterLogger {
	return &DeadLetterLogger{}
}

func (d *DeadLetterLogger) Deliver(ctx context.Context, msg *redis.Msg) error {
	fmt.Printf("msg fail execeed retry limit, msg id: %s", msg.MsgId)
	return nil
}

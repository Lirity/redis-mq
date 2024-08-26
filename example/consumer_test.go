package example

import (
	"context"
	"fmt"
	redis_mq "redis-mq"
	"redis-mq/redis"
	"testing"
	"time"
)

func Test_Consumer(t *testing.T) {
	client := redis.NewClient()
	msgCallback := func(ctx context.Context, msg *redis.Msg) error {
		fmt.Printf("message: %s, %s", msg.Key, msg.Val)
		return nil
	}
	deadLetterLogger := redis_mq.NewDeadLetterLogger()
	consumer := redis_mq.NewConsumer(client, "topic", "group", "consumer", msgCallback, deadLetterLogger)
	defer consumer.Stop()
	<-time.After(10 * time.Second)
}

package example

import (
	"os"
	"os/signal"
	redis_mq "redis-mq"
	"redis-mq/redis"
	"syscall"
	"testing"
)

func Test_Producer(t *testing.T) {
	client := redis.NewClient()
	redis_mq.NewProducer(client, "topic", 10)
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
}

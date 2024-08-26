package redis

import (
	"context"
	"testing"
)

func Test_redis_xadd(t *testing.T) {
	client := NewClient()
	ctx := context.Background()
	res, err := client.XADD(ctx, "topic", 100, map[string]string{"test_key": "test_value"})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(res)
}

func Test_redis_xreadgroup(t *testing.T) {
	client := NewClient()
	ctx := context.Background()
	res, err := client.XREADGROUP(ctx, "group", "consumer", "topic", 5, "0-0")
	if err != nil {
		t.Error(err)
	}
	t.Log(len(res))
}

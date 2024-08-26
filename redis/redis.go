package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

const (
	DefaultRedisAddr       = "127.0.0.1:6379"
	DefaultRedisPassword   = ""
	DefaultRedisDB         = 0
	DefaultPoolSize        = 10
	DefaultMinIdleConns    = 3
	DefaultConnMaxIdleTime = 5 * time.Minute
	DefaultConnMaxLifeTime = 30 * time.Minute
)

type Msg struct {
	MsgId string
	Key   string
	Val   string
}

type Client struct {
	rdb *redis.Client
}

func NewClient() *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     DefaultRedisAddr,
		Password: DefaultRedisPassword,
		DB:       DefaultRedisDB,

		// 连接池配置
		PoolSize:        DefaultPoolSize,        // 连接池的最大连接数
		MinIdleConns:    DefaultMinIdleConns,    // 连接池最小空闲连接数
		ConnMaxIdleTime: DefaultConnMaxIdleTime, // 连接的最大空闲时间
		ConnMaxLifetime: DefaultConnMaxLifeTime, // 连接的最大生命周期
	})

	return &Client{rdb: rdb}
}

// XADD 向topic中添加数据
func (client *Client) XADD(ctx context.Context, topic string, maxLen int64, message map[string]string) (string, error) {
	if topic == "" {
		return "", errors.New("redis xadd: topic can't be empty")
	}
	res, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		ID:     "*", // ID自增
		MaxLen: maxLen,
		Values: message,
	}).Result()
	if err != nil {
		return "", err
	}
	return res, nil
}

// XREADGROUP 从topic中消费数据
func (client *Client) XREADGROUP(ctx context.Context, group, consumer, topic string, timeoutMiliSeconds int, typ string) ([]*Msg, error) {
	if group == "" || consumer == "" || topic == "" {
		return nil, errors.New("redis xreadgroup: group | consumer | topic can't be empty")
	}
	if typ != ">" && typ != "0-0" {
		return nil, errors.New("redis xreadgroup: typ should be '>' or '0-0'")
	}

	var msgs []*Msg
	res, err := client.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{topic, typ},
		//Count:    10,                                // 一次最多取多少消息
		Block: time.Duration(timeoutMiliSeconds), // 阻塞超时时间 0为永久阻塞 -1为不阻塞
	}).Result()
	if err != nil {
		return nil, err
	}

	for _, stream := range res {
		for _, message := range stream.Messages {
			for key, val := range message.Values {
				valStr, _ := val.(string)
				msgs = append(msgs, &Msg{
					MsgId: message.ID,
					Key:   key,
					Val:   valStr,
				})
			}
		}
	}

	return msgs, nil
}

// XACK 以组为单位确认消息
func (client *Client) XACK(ctx context.Context, topic, group string, msgId ...string) error {
	if topic == "" || group == "" || len(msgId) == 0 {
		return errors.New("redis xack: topic | group | msgId can't be empty")
	}
	res, err := client.rdb.XAck(ctx, topic, group, msgId...).Result()
	if err != nil {
		return err
	}
	fmt.Println(res)
	return nil
}

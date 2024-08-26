go get github.com/redis/go-redis/v9

基于Redis Stream + go-redis实现的消息队列

1.redis包基于go-redis提供了redis连接 封装了XADD XREADGROUP XACK方法
2.redis_mq包封装了consumer和producer

1.创建Topic
```
localhost:6379> XADD topic * key val
"1724556216679-0"
```
2.创建消费者组
```
localhost:6379> XGROUP CREATE topic group 0-0
"OK"
```
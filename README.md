# go-redis

Common Redis Client for Golang Microservices
===========================================

This library provides a powerful, easy-to-use Redis client with full support for popular features in Golang microservices.

## Key Features
- Supports Standalone, Cluster, and Sentinel connection modes
- Full API for String, List, Set, Hash, Sorted Set, Stream, Pub/Sub
- Supports distributed lock (RedSync)
- Supports cache patterns: Cache-Aside, Write-Through, Write-Behind
- Integrated metrics for performance monitoring
- Easy to extend, test, and mock

## Installation

```sh
go get github.com/gotech-hub/go-redis
```

## Configuration

```go
cfg := &redis.RedisConfig{
    Mode:      "standalone", // or "cluster", "sentinel"
    Addr:      "localhost:6379",
    Password:  "",
    DB:        0,
    // ... other options ...
}
```

## Initialize client

```go
client, err := redis.ConnectRedis(context.Background(), cfg)
if err != nil {
    panic(err)
}
```

## Usage by function group

### String & Struct
```go
err = client.SetString(ctx, "key", "value", 10*time.Second)
val, err := client.GetString(ctx, "key")

err = client.SetStruct(ctx, "user:1", user, 10*time.Second)
err = client.GetStruct(ctx, "user:1", &userResult)
```

### List
```go
_, err := client.LPush(ctx, "list", "a", "b")
vals, err := client.LRange(ctx, "list", 0, -1)
```

### Set
```go
_, err := client.SAdd(ctx, "set", "x", "y")
members, err := client.SMembers(ctx, "set")
```

### Hash
```go
_, err := client.HSet(ctx, "hash", "field", "val")
hval, err := client.HGet(ctx, "hash", "field")
```

### Sorted Set
```go
_, err := client.ZAdd(ctx, "zset", &redis.Z{Score: 1, Member: "one"})
zvals, err := client.ZRange(ctx, "zset", 0, -1)
```

### Stream
```go
id, err := client.XAdd(ctx, "stream", map[string]interface{}{"foo": "bar"})
streams, err := client.XRead(ctx, []string{"stream"}, []string{"0"}, 1, 0)
```

### Pub/Sub
```go
pubsub, err := client.Subscribe(ctx, func(ctx context.Context, msg *redis.Message) error {
    fmt.Println(msg.Channel, msg.Data)
    return nil
}, "my-channel")
err = client.Publish(ctx, "my-channel", "hello")
```

### Distributed Lock
```go
mutex := client.NewMutex("lock:mykey", 5*time.Second)
err := mutex.Lock()
// ... critical section ...
_, err = mutex.Unlock()
```

### Cache Pattern
```go
val, err := client.CacheAside(ctx, "cache:key", 10*time.Second, func() (interface{}, error) {
    // Get data from DB or API
    return "data-from-db", nil
})
```

## Metrics
- Built-in metrics for each command, cache hit/miss, connection pool, etc.
- You can get a metrics snapshot via client.metrics.GetMetricsSnapshot()

## Contribution
- PRs, issues, and feedback are welcome!

## License
MIT

---

## List of Supported Functions & Detailed Descriptions

| Function Group | Function Name | Description |
|---|---|---|
| Connection | `ConnectRedis(ctx, cfg)` | Initialize Redis connection (standalone/cluster/sentinel) |
| String & Struct | `SetString`, `GetString`, `SetStruct`, `GetStruct` | Store/retrieve string or struct data (marshal/unmarshal JSON) |
| List | `LPush`, `RPush`, `LPop`, `RPop`, `LRange` | Queue, stack operations, retrieve multiple elements |
| Set | `SAdd`, `SMembers`, `SIsMember`, `SRem`, `SCard` | Store unique sets, check membership |
| Hash | `HSet`, `HGet`, `HGetAll`, `HDel`, `HExists`, `HKeys`, `HIncrBy` | Store key-value objects, suitable for profile/user/session |
| Sorted Set | `ZAdd`, `ZRange`, `ZRangeWithScores`, `ZRangeByScore`, `ZRank`, `ZScore`, `ZRem`, `ZIncrBy`, `ZCard`, `ZCount` | Support for ranking, leaderboard, prioritization |
| Stream | `XAdd`, `XRead`, `XGroupCreate`, `XReadGroup`, `XAck`, `XPending`, `XClaim`, `XTrim`, `XRange`, `XRevRange`, `XLen` | Durable message queue, consumer group support |
| Pub/Sub | `Publish`, `Subscribe`, `SubscribeWithTopicFilter`, `SubscribeWithPattern` | Real-time communication between services |
| Cache Pattern | `CacheAside`, `WriteThrough`, `WriteBehind`, `BatchGet`, `Refresh`, `DeleteWithWriteThrough`, `DeleteWithWriteBehind` | Popular cache patterns for microservices |
| Lock | `NewMutex`, `SetWithLock`, `GetWithLock` | Distributed lock, ensure safe concurrent operations |
| Metrics | `GetMetricsSnapshot` | Track command count, cache hit/miss, connection pool, etc. |

### Notes
- All functions accept `ctx context.Context` to support timeout, cancel, and trace.
- All functions return clear errors for easy debugging.
- You can extend with more function groups as needed.

---
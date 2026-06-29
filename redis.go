package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	expDefault      = 60 * time.Second
	expMutexDefault = 10 * time.Second
)

// Client represents a Redis client wrapper
type Client struct {
	client          redis.UniversalClient
	redSync         *redsync.Redsync
	expDefault      time.Duration
	expMutexDefault time.Duration
	// Optional metrics collection
	metrics *RedisMetrics
}

var (
	instanceRedisClient *Client
	onceRedisClient     sync.Once
)

// Close releases the underlying Redis connection pool. Safe to call on a
// zero-value Client.
func (c *Client) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// ConnectRedis creates a connection to Redis server (standalone, cluster, or sentinel)
func ConnectRedis(ctx context.Context, cfg *RedisConfig) (*Client, error) {
	// Return existing instance if available and connected
	if instanceRedisClient != nil {
		_, err := instanceRedisClient.client.Ping(ctx).Result()
		if err == nil {
			return instanceRedisClient, nil
		}
	}

	onceRedisClient.Do(func() {
		var redisClient redis.UniversalClient

		switch cfg.Mode {
		case "cluster":
			// Connect to Redis Cluster
			redisClient = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    cfg.Addresses,
				Password: cfg.Password,
				Username: cfg.User,
			})
		case "sentinel":
			// Connect to Redis Sentinel
			redisClient = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    cfg.MasterName,
				SentinelAddrs: cfg.Addresses,
				Password:      cfg.Password,
				Username:      cfg.User,
			})
		default:
			// Connect to standalone Redis
			redisClient = redis.NewClient(&redis.Options{
				Addr:     cfg.Addr,
				DB:       cfg.DB,
				Password: cfg.Password,
				Username: cfg.User,
			})
		}

		// Test connection
		_, err := redisClient.Ping(ctx).Result()
		if err != nil {
			log.Error().Err(err).Msg("ping redis failed")
			instanceRedisClient = &Client{client: nil}
			return
		}

		// Create redsync for distributed locking
		pool := goredis.NewPool(redisClient)
		redisRedsync := redsync.New(pool)

		log.Info().
			Str("mode", cfg.Mode).
			Str("addr", cfg.Addr).
			Msg("connect redis successfully")

		// Set default expiration times
		expDefault := expDefault
		if cfg.DefaultExpiration > 0 {
			expDefault = cfg.DefaultExpiration
		}

		expMutexDefault := expMutexDefault
		if cfg.DefaultMutexExpiration > 0 {
			expMutexDefault = cfg.DefaultMutexExpiration
		}

		// Create metrics only if needed
		var metrics *RedisMetrics
		metrics = NewRedisMetrics()

		instanceRedisClient = &Client{
			client:          redisClient,
			redSync:         redisRedsync,
			expDefault:      expDefault,
			expMutexDefault: expMutexDefault,
			metrics:         metrics,
		}
	})

	return instanceRedisClient, nil
}

// GetInstance returns the singleton Redis client instance
func GetInstance() *Client {
	return instanceRedisClient
}

// GetClient returns the underlying go-redis client
func (c *Client) GetClient() redis.UniversalClient {
	return c.client
}

// SetString sets a string value in Redis
func (c *Client) SetString(ctx context.Context, key string, value string, exp time.Duration) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	if exp == 0 {
		exp = c.expDefault
	}
	return c.client.Set(ctx, key, value, exp).Err()
}

// GetString retrieves a string value from Redis
func (c *Client) GetString(ctx context.Context, key string) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	val, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return val, err
}

// SetStruct marshals a struct and stores it in Redis
func (c *Client) SetStruct(ctx context.Context, key string, value interface{}, exp time.Duration) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if exp == 0 {
		exp = c.expDefault
	}
	return c.client.Set(ctx, key, data, exp).Err()
}

// GetStruct retrieves a value from Redis and unmarshals it into the provided struct
func (c *Client) GetStruct(ctx context.Context, key string, dest interface{}) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	value, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(value, dest)
}

// Delete removes a key from Redis
func (c *Client) Delete(ctx context.Context, key string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	return c.client.Del(ctx, key).Err()
}

// DeleteWithPattern removes all keys matching a pattern
func (c *Client) DeleteWithPattern(ctx context.Context, pattern string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		if err := c.Delete(ctx, key); err != nil {
			return err
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

// NewMutex creates a distributed lock using redsync
func (c *Client) NewMutex(key string, exp time.Duration) *redsync.Mutex {
	if c.redSync == nil {
		return nil
	}
	if exp == 0 {
		exp = c.expMutexDefault
	}
	return c.redSync.NewMutex(key, redsync.WithExpiry(exp))
}

// Exists checks if a key exists in Redis
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c.client == nil {
		return false, errors.New("redis client is nil")
	}
	result, err := c.client.Exists(ctx, key).Result()
	return result > 0, err
}

// TTL returns the remaining time-to-live of a key
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	return c.client.TTL(ctx, key).Result()
}

// LPush thêm một hoặc nhiều giá trị vào đầu list
func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.LPush(ctx, key, values...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("LPush", start, err)
	}
	return result, err
}

// RPush thêm một hoặc nhiều giá trị vào cuối list
func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.RPush(ctx, key, values...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("RPush", start, err)
	}
	return result, err
}

// LPop lấy và xóa phần tử đầu tiên từ list
func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.LPop(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("LPop", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return result, err
}

// RPop lấy và xóa phần tử cuối cùng từ list
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.RPop(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("RPop", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return result, err
}

// LRange lấy một phần list từ start đến stop (zero-based)
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	startTime := time.Now()
	result, err := c.client.LRange(ctx, key, start, stop).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("LRange", startTime, err)
	}
	return result, err
}

// LLen lấy độ dài của list
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.LLen(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("LLen", start, err)
	}
	return result, err
}

// BLPop block và lấy phần tử đầu tiên từ list, timeout là thời gian chờ tối đa
func (c *Client) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.BLPop(ctx, timeout, keys...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("BLPop", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return result, err
}

// BRPop block và lấy phần tử cuối cùng từ list, timeout là thời gian chờ tối đa
func (c *Client) BRPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.BRPop(ctx, timeout, keys...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("BRPop", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return result, err
}

// ---- Hash Operations ----

// HSet thiết lập một hoặc nhiều field-value trong hash
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HSet(ctx, key, values...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HSet", start, err)
	}
	return result, err
}

// HSetStruct thiết lập một struct làm hash, sử dụng tên field từ struct tags
func (c *Client) HSetStruct(ctx context.Context, key string, value interface{}) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	start := time.Now()

	// Serialize struct thành map để lưu vào Redis hash
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	// Chuyển JSON thành map
	var valueMap map[string]interface{}
	if err := json.Unmarshal(data, &valueMap); err != nil {
		return err
	}

	// Chuyển map thành cặp field/value
	var args []interface{}
	for k, v := range valueMap {
		args = append(args, k, v)
	}

	_, err = c.client.HSet(ctx, key, args...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HSetStruct", start, err)
	}
	return err
}

// HGet lấy giá trị của field từ hash
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HGet(ctx, key, field).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HGet", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return result, err
}

// HGetAll lấy tất cả field-value từ hash
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HGetAll(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HGetAll", start, err)
	}
	return result, err
}

// HGetStruct lấy hash và deserialize thành struct
func (c *Client) HGetStruct(ctx context.Context, key string, dest interface{}) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	start := time.Now()

	// Lấy tất cả field-value từ hash
	values, err := c.client.HGetAll(ctx, key).Result()
	if err != nil {
		if c.metrics != nil {
			c.metrics.TrackCommand("HGetStruct", start, err)
		}
		return err
	}

	if len(values) == 0 {
		if c.metrics != nil {
			c.metrics.TrackCommand("HGetStruct", start, nil)
		}
		return nil
	}

	// Serialize thành JSON và deserialize vào struct
	jsonData, err := json.Marshal(values)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonData, dest)
	if c.metrics != nil {
		c.metrics.TrackCommand("HGetStruct", start, err)
	}
	return err
}

// HDel xóa một hoặc nhiều field từ hash
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HDel(ctx, key, fields...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HDel", start, err)
	}
	return result, err
}

// HExists kiểm tra field có tồn tại trong hash không
func (c *Client) HExists(ctx context.Context, key, field string) (bool, error) {
	if c.client == nil {
		return false, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HExists(ctx, key, field).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HExists", start, err)
	}
	return result, err
}

// HKeys lấy tất cả field từ hash
func (c *Client) HKeys(ctx context.Context, key string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HKeys(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HKeys", start, err)
	}
	return result, err
}

// HIncrBy tăng giá trị số của field trong hash
func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.HIncrBy(ctx, key, field, incr).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("HIncrBy", start, err)
	}
	return result, err
}

// ---- Set Operations ----

// SAdd thêm một hoặc nhiều member vào set
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SAdd(ctx, key, members...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SAdd", start, err)
	}
	return result, err
}

// SMembers lấy tất cả member từ set
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SMembers(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SMembers", start, err)
	}
	return result, err
}

// SIsMember kiểm tra member có tồn tại trong set không
func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	if c.client == nil {
		return false, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SIsMember(ctx, key, member).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SIsMember", start, err)
	}
	return result, err
}

// SRem xóa một hoặc nhiều member từ set
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SRem(ctx, key, members...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SRem", start, err)
	}
	return result, err
}

// SCard lấy số lượng member trong set
func (c *Client) SCard(ctx context.Context, key string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SCard(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SCard", start, err)
	}
	return result, err
}

// SPop lấy và xóa một hoặc nhiều member ngẫu nhiên từ set
func (c *Client) SPop(ctx context.Context, key string) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SPop(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SPop", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return result, err
}

// SPopN lấy và xóa count member ngẫu nhiên từ set
func (c *Client) SPopN(ctx context.Context, key string, count int64) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SPopN(ctx, key, count).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SPopN", start, err)
	}
	return result, err
}

// SDiff trả về set difference của các set
func (c *Client) SDiff(ctx context.Context, keys ...string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SDiff(ctx, keys...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SDiff", start, err)
	}
	return result, err
}

// SInter trả về set intersection của các set
func (c *Client) SInter(ctx context.Context, keys ...string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SInter(ctx, keys...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SInter", start, err)
	}
	return result, err
}

// SUnion trả về set union của các set
func (c *Client) SUnion(ctx context.Context, keys ...string) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.SUnion(ctx, keys...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("SUnion", start, err)
	}
	return result, err
}

// ---- Sorted Set Operations ----

// ZAdd thêm một hoặc nhiều member với score vào sorted set
func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZAdd(ctx, key, members...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZAdd", start, err)
	}
	return result, err
}

// ZRange lấy member từ sorted set theo range index
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	startTime := time.Now()
	result, err := c.client.ZRange(ctx, key, start, stop).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRange", startTime, err)
	}
	return result, err
}

// ZRangeWithScores lấy member và score từ sorted set theo range index
func (c *Client) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	startTime := time.Now()
	result, err := c.client.ZRangeWithScores(ctx, key, start, stop).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRangeWithScores", startTime, err)
	}
	return result, err
}

// ZRangeByScore lấy member từ sorted set theo range score
func (c *Client) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZRangeByScore(ctx, key, opt).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRangeByScore", start, err)
	}
	return result, err
}

// ZRangeByScoreWithScores lấy member và score từ sorted set theo range score
func (c *Client) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) ([]redis.Z, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZRangeByScoreWithScores(ctx, key, opt).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRangeByScoreWithScores", start, err)
	}
	return result, err
}

// ZRank lấy rank của member trong sorted set (0-based, theo thứ tự tăng dần)
func (c *Client) ZRank(ctx context.Context, key, member string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZRank(ctx, key, member).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRank", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return -1, nil
	}
	return result, err
}

// ZRevRank lấy rank của member trong sorted set (0-based, theo thứ tự giảm dần)
func (c *Client) ZRevRank(ctx context.Context, key, member string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZRevRank(ctx, key, member).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRevRank", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return -1, nil
	}
	return result, err
}

// ZScore lấy score của member trong sorted set
func (c *Client) ZScore(ctx context.Context, key, member string) (float64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZScore(ctx, key, member).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZScore", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return result, err
}

// ZRem xóa một hoặc nhiều member từ sorted set
func (c *Client) ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZRem(ctx, key, members...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZRem", start, err)
	}
	return result, err
}

// ZIncrBy tăng score của member trong sorted set
func (c *Client) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZIncrBy(ctx, key, increment, member).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZIncrBy", start, err)
	}
	return result, err
}

// ZCard lấy số lượng member trong sorted set
func (c *Client) ZCard(ctx context.Context, key string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZCard(ctx, key).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZCard", start, err)
	}
	return result, err
}

// ZCount đếm số member trong sorted set với score trong range
func (c *Client) ZCount(ctx context.Context, key, min, max string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()
	result, err := c.client.ZCount(ctx, key, min, max).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("ZCount", start, err)
	}
	return result, err
}

// ---- Stream Operations ----

// XAdd thêm message vào stream
func (c *Client) XAdd(ctx context.Context, stream string, values map[string]interface{}) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	start := time.Now()

	args := &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}

	result, err := c.client.XAdd(ctx, args).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XAdd", start, err)
	}
	return result, err
}

// XRead đọc messages từ một hoặc nhiều streams
func (c *Client) XRead(ctx context.Context, streams []string, ids []string, count int64, block time.Duration) ([]redis.XStream, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()

	args := &redis.XReadArgs{
		Streams: streams,
		Count:   count,
		Block:   block,
	}
	// Thêm IDs vào cuối slice streams theo yêu cầu của Redis
	args.Streams = append(args.Streams, ids...)

	result, err := c.client.XRead(ctx, args).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XRead", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return result, err
}

// XGroupCreate tạo consumer group mới cho stream
func (c *Client) XGroupCreate(ctx context.Context, stream, group, start string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	startTime := time.Now()

	err := c.client.XGroupCreate(ctx, stream, group, start).Err()
	if c.metrics != nil {
		c.metrics.TrackCommand("XGroupCreate", startTime, err)
	}
	if err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists" {
		return nil // Bỏ qua lỗi nếu group đã tồn tại
	}
	return err
}

// XReadGroup đọc messages từ stream thông qua consumer group
func (c *Client) XReadGroup(ctx context.Context, group, consumer string, streams []string, ids []string, count int64, block time.Duration) ([]redis.XStream, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()

	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    count,
		Block:    block,
	}
	// Thêm IDs vào cuối slice streams
	args.Streams = append(args.Streams, ids...)

	result, err := c.client.XReadGroup(ctx, args).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XReadGroup", start, err)
	}
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return result, err
}

// XACK xác nhận message đã được xử lý trong consumer group
func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()

	result, err := c.client.XAck(ctx, stream, group, ids...).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XAck", start, err)
	}
	return result, err
}

// XPending lấy thông tin về pending messages trong consumer group
func (c *Client) XPending(ctx context.Context, stream, group string) (*redis.XPending, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()

	result, err := c.client.XPending(ctx, stream, group).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XPending", start, err)
	}
	return result, err
}

// XClaim lấy lại message từ một consumer khác trong group
func (c *Client) XClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, messages []string) ([]redis.XMessage, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	start := time.Now()

	args := &redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: messages,
	}

	result, err := c.client.XClaim(ctx, args).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XClaim", start, err)
	}
	return result, err
}

// XTrim cắt bớt stream để giới hạn kích thước
func (c *Client) XTrim(ctx context.Context, stream string, maxLen int64) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()

	result, err := c.client.XTrimMaxLen(ctx, stream, maxLen).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XTrim", start, err)
	}
	return result, err
}

// XRange lấy messages từ stream trong một range ID
func (c *Client) XRange(ctx context.Context, stream, start, stop string) ([]redis.XMessage, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	startTime := time.Now()

	result, err := c.client.XRange(ctx, stream, start, stop).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XRange", startTime, err)
	}
	return result, err
}

// XRevRange lấy messages từ stream trong một range ID theo thứ tự ngược
func (c *Client) XRevRange(ctx context.Context, stream, start, stop string) ([]redis.XMessage, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}
	startTime := time.Now()

	result, err := c.client.XRevRange(ctx, stream, start, stop).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XRevRange", startTime, err)
	}
	return result, err
}

// XLen lấy số lượng messages trong stream
func (c *Client) XLen(ctx context.Context, stream string) (int64, error) {
	if c.client == nil {
		return 0, errors.New("redis client is nil")
	}
	start := time.Now()

	result, err := c.client.XLen(ctx, stream).Result()
	if c.metrics != nil {
		c.metrics.TrackCommand("XLen", start, err)
	}
	return result, err
}

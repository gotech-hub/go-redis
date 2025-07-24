package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	logger "github.com/gotech-hub/go-logging"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

const (
	expDefault      = 60 * time.Second
	expMutexDefault = 10 * time.Second
)

type Client struct {
	client          redis.UniversalClient
	redSync         *redsync.Redsync
	expDefault      time.Duration
	expMutexDefault time.Duration
}

var (
	instanceRedisClient *Client
	onceRedisClient     sync.Once
)

func ConnectRedis(ctx context.Context, cfg *RedisConfig) (*Client, error) {
	log := logger.GetLogger()

	if instanceRedisClient != nil {
		_, err := instanceRedisClient.client.Ping(ctx).Result()
		if err == nil {
			return instanceRedisClient, nil
		}
	}

	onceRedisClient.Do(func() {
		redisClient := redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			DB:       0,
			Password: cfg.Password,
			Username: cfg.User,
		})

		_, err := redisClient.Ping(ctx).Result()
		if err != nil {
			log.Error().Err(err).Msg("ping redis failed")
			instanceRedisClient = &Client{
				client:          nil,
				redSync:         nil,
				expDefault:      0,
				expMutexDefault: 0,
			}
			return
		}

		pool := goredis.NewPool(redisClient)
		// Create an instance of redisync to be used to obtain a mutual exclusion lock.
		redisRedsync := redsync.New(pool)

		log.Info().Msg("connect redis successfully")

		instanceRedisClient = &Client{
			client:          redisClient,
			redSync:         redisRedsync,
			expDefault:      expDefault,
			expMutexDefault: expMutexDefault,
		}
	})

	return instanceRedisClient, nil
}

func GetInstance() *Client {
	return instanceRedisClient
}

func (c *Client) GetClient() redis.UniversalClient {
	return c.client
}

func (c *Client) GetDataCache(ctx context.Context, key string, rs interface{}) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}

	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		return err
	} else {
		err = json.Unmarshal(data, &rs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) SetDataCache(ctx context.Context, key string, value interface{}, exp time.Duration) error {
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

func (c *Client) IncrementDataCache(ctx context.Context, key string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	return c.client.Incr(ctx, key).Err()
}

func (c *Client) DecrementDataCache(ctx context.Context, key string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	value, err := c.client.Get(ctx, key).Int64()
	if err == nil && value >= 0 {
		return c.client.Incr(ctx, key).Err()
	}

	return nil
}

func (c *Client) RemoteDataCache(ctx context.Context, key string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	return c.client.Del(ctx, key).Err()
}

func (c *Client) SetString(ctx context.Context, key string, value string, exp time.Duration) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	if exp == 0 {
		exp = c.expDefault
	}
	return c.client.Set(ctx, key, value, exp).Err()
}

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

func (c *Client) GetString(ctx context.Context, key string) (string, error) {
	if c.client == nil {
		return "", errors.New("redis client is nil")
	}
	val, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return val, nil
}

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

	if err = json.Unmarshal(value, dest); err != nil {
		return err
	}

	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	return c.client.Del(ctx, key).Err()
}

func (c *Client) DeleteWithPattern(ctx context.Context, pattern string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		err := c.Delete(ctx, key)
		if err != nil {
			return err
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}

	return nil
}

func (c *Client) NewMutex(key string, exp time.Duration) *redsync.Mutex {
	if c.redSync == nil {
		return nil
	}
	if exp == 0 {
		exp = c.expMutexDefault
	}
	return c.redSync.NewMutex(key, redsync.WithExpiry(exp))
}

type RepoFuncGet func() (interface{}, error)

func (c *Client) GetCacheWithReadThrough(ctx context.Context, key string, exp time.Duration, dest interface{}, repoFuncGet RepoFuncGet, useRedlock bool) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	if repoFuncGet == nil {
		return errors.New("RepoFuncGet is nil")
	}

	log := logger.GetLogger().AddTraceInfoContextRequest(ctx)

	value, err := c.client.Get(ctx, key).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Warn().Err(err).Msg("redis error")
	}

	if len(value) != 0 {
		if err := json.Unmarshal(value, dest); err != nil {
			log.Warn().Err(err).Msg("redis unmarshal error")
		} else {
			return nil
		}
	}

	if useRedlock {
		keyLock := key + ":lock"
		mutex := c.NewMutex(keyLock, 10*time.Second)
		if err := mutex.LockContext(ctx); err != nil {
			log.Warn().Err(err).Msgf("redis mutex lock key=%s error", keyLock)
		} else {
			defer func() {
				if _, err := mutex.UnlockContext(ctx); err != nil {
					log.Warn().Err(err).Msgf("redis mutex unlock key=%s error", keyLock)
				}
			}()

			value, err = c.client.Get(ctx, key).Bytes()
			if err != nil && !errors.Is(err, redis.Nil) {
				log.Warn().Err(err).Msg("redis error")
			}

			if len(value) != 0 {
				if err := json.Unmarshal(value, dest); err != nil {
					log.Warn().Err(err).Msg("redis unmarshal error")
				} else {
					return nil
				}
			}
		}
	}

	data, err := repoFuncGet()
	if err != nil {
		return err
	}

	if data != nil && !reflect.ValueOf(data).IsNil() {
		if err := c.SetStruct(ctx, key, data, exp); err != nil {
			log.Warn().Err(err).Msg("redis set error")
		}

		byteData, _ := json.Marshal(data)
		if err := json.Unmarshal(byteData, dest); err != nil {
			log.Warn().Err(err).Msg("unmarshal error")
		}
	}

	return nil
}

func (c *Client) AcquireLock(ctx context.Context, lockKey string, lockTimeout time.Duration) (bool, error) {
	if c.client == nil {
		return false, errors.New("redis client is nil")
	}
	isSet, err := c.client.SetNX(ctx, lockKey, 1, lockTimeout).Result()
	if err != nil {
		return false, err
	}

	return isSet, err
}

func (c *Client) ReleaseLock(ctx context.Context, lockKey string) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}
	_, err := c.client.Del(ctx, lockKey).Result()
	if err != nil {
		return err
	}

	return nil
}

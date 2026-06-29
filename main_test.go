package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type User struct {
	ID   int
	Name string
}

func TestRedisClientBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: requires a live Redis server")
	}
	ctx := context.Background()
	cfg := &RedisConfig{
		Mode:                   "standalone",
		Addr:                   "localhost:6379",
		Password:               "pass",
		User:                   "",
		DB:                     0,
		ReadTimeout:            3 * time.Second,
		WriteTimeout:           3 * time.Second,
		DefaultExpiration:      60 * time.Second,
		DefaultMutexExpiration: 10 * time.Second,
	}
	client, err := ConnectRedis(ctx, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Test SetString & GetString
	err = client.SetString(ctx, "hello", "world", 0)
	assert.NoError(t, err)
	val, err := client.GetString(ctx, "hello")
	assert.NoError(t, err)
	assert.Equal(t, "world", val)

	// Test SetStruct & GetStruct
	user := User{ID: 1, Name: "Alice"}
	err = client.SetStruct(ctx, "user:1", user, 0)
	assert.NoError(t, err)
	var userResult User
	err = client.GetStruct(ctx, "user:1", &userResult)
	assert.NoError(t, err)
	assert.Equal(t, user, userResult)

	// Test Exists
	exists, err := client.Exists(ctx, "hello")
	assert.NoError(t, err)
	assert.True(t, exists)

	// Test TTL
	ttl, err := client.TTL(ctx, "hello")
	assert.NoError(t, err)
	assert.True(t, ttl > 0 || ttl == -1)

	// Test Delete
	err = client.Delete(ctx, "hello")
	assert.NoError(t, err)
	val, err = client.GetString(ctx, "hello")
	assert.NoError(t, err)
	assert.Equal(t, "", val)

	// Test DeleteWithPattern
	err = client.SetString(ctx, "test:1", "a", 0)
	assert.NoError(t, err)
	err = client.SetString(ctx, "test:2", "b", 0)
	assert.NoError(t, err)
	err = client.DeleteWithPattern(ctx, "test:*")
	assert.NoError(t, err)

	// Test Mutex (distributed lock)
	mutex := client.NewMutex("lock:test", 5*time.Second)
	if assert.NotNil(t, mutex) {
		err = mutex.Lock()
		assert.NoError(t, err)
		_, err = mutex.Unlock()
		assert.NoError(t, err)
	}
}

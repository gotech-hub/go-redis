package redis

import (
	"time"
)

// RedisConfig cấu hình kết nối đến Redis
type RedisConfig struct {
	// Cấu hình chung
	Mode      string   `yaml:"mode" json:"mode"`           // standalone, cluster, sentinel
	Addr      string   `yaml:"addr" json:"addr"`           // Cho standalone
	Addresses []string `yaml:"addresses" json:"addresses"` // Cho cluster hoặc sentinel
	Password  string   `yaml:"password" json:"password"`
	User      string   `yaml:"user" json:"user"`
	DB        int      `yaml:"db" json:"db"`

	// Tuỳ chọn mở rộng cho Sentinel mode
	MasterName string `yaml:"master_name" json:"master_name"` // Master name cho Redis Sentinel

	// Cấu hình timeout cơ bản
	ReadTimeout  time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout" json:"write_timeout"`

	// Expiration settings
	DefaultExpiration      time.Duration `yaml:"default_expiration" json:"default_expiration"`
	DefaultMutexExpiration time.Duration `yaml:"default_mutex_expiration" json:"default_mutex_expiration"`
}

// NewDefaultRedisConfig tạo cấu hình mặc định cho Redis
func NewDefaultRedisConfig() *RedisConfig {
	return &RedisConfig{
		Mode:                   "standalone",
		Addr:                   "localhost:6379",
		DB:                     0,
		ReadTimeout:            3 * time.Second,
		WriteTimeout:           3 * time.Second,
		DefaultExpiration:      60 * time.Second,
		DefaultMutexExpiration: 10 * time.Second,
	}
}

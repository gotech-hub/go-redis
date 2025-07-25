package redis

import (
	"sync"
	"sync/atomic"
	"time"
)

// RedisMetrics theo dõi hiệu suất Redis operations
type RedisMetrics struct {
	// Thống kê command usage
	commandCount      map[string]*atomic.Int64
	commandErrorCount map[string]*atomic.Int64

	// Thời gian thực thi trung bình (ms)
	commandLatency map[string]*RollingAverage

	// Hit/miss ratio cho cache operations
	cacheHits   *atomic.Int64
	cacheMisses *atomic.Int64

	// Mutual exclusion lock
	mu sync.RWMutex
}

// RollingAverage tính thời gian trung bình theo cửa sổ trượt
type RollingAverage struct {
	values []float64
	sum    float64
	size   int
	index  int
	count  int
	mu     sync.Mutex
}

// NewRollingAverage khởi tạo cửa sổ trượt mới
func NewRollingAverage(size int) *RollingAverage {
	return &RollingAverage{
		values: make([]float64, size),
		size:   size,
	}
}

// Add thêm giá trị vào cửa sổ trượt
func (r *RollingAverage) Add(value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count < r.size {
		r.sum += value
		r.values[r.count] = value
		r.count++
	} else {
		r.sum -= r.values[r.index]
		r.sum += value
		r.values[r.index] = value
		r.index = (r.index + 1) % r.size
	}
}

// Average trả về giá trị trung bình
func (r *RollingAverage) Average() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return 0
	}
	return r.sum / float64(r.count)
}

// NewRedisMetrics khởi tạo Redis metrics collector
func NewRedisMetrics() *RedisMetrics {
	return &RedisMetrics{
		commandCount:      make(map[string]*atomic.Int64),
		commandErrorCount: make(map[string]*atomic.Int64),
		commandLatency:    make(map[string]*RollingAverage),
		cacheHits:         &atomic.Int64{},
		cacheMisses:       &atomic.Int64{},
	}
}

// TrackCommand ghi nhận thời gian thực thi command và kết quả
func (m *RedisMetrics) TrackCommand(cmd string, start time.Time, err error) {
	// Ghi nhận số lượng command
	m.mu.RLock()
	counter, exists := m.commandCount[cmd]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		counter, exists = m.commandCount[cmd]
		if !exists {
			counter = &atomic.Int64{}
			m.commandCount[cmd] = counter

			// Khởi tạo error counter
			m.commandErrorCount[cmd] = &atomic.Int64{}

			// Khởi tạo latency tracker
			m.commandLatency[cmd] = NewRollingAverage(100)
		}
		m.mu.Unlock()
	}

	counter.Add(1)

	// Ghi nhận lỗi nếu có
	if err != nil {
		m.mu.RLock()
		errorCounter := m.commandErrorCount[cmd]
		m.mu.RUnlock()
		errorCounter.Add(1)
	}

	// Ghi nhận thời gian thực thi
	latency := time.Since(start).Milliseconds()

	m.mu.RLock()
	latencyTracker := m.commandLatency[cmd]
	m.mu.RUnlock()

	latencyTracker.Add(float64(latency))
}

// TrackCacheHit ghi nhận cache hit
func (m *RedisMetrics) TrackCacheHit() {
	m.cacheHits.Add(1)
}

// TrackCacheMiss ghi nhận cache miss
func (m *RedisMetrics) TrackCacheMiss() {
	m.cacheMisses.Add(1)
}

// GetCacheHitRatio trả về tỷ lệ cache hit
func (m *RedisMetrics) GetCacheHitRatio() float64 {
	hits := m.cacheHits.Load()
	misses := m.cacheMisses.Load()

	if hits+misses == 0 {
		return 0
	}

	return float64(hits) / float64(hits+misses)
}

// GetMetricsSnapshot trả về snapshot các metrics hiện tại
func (m *RedisMetrics) GetMetricsSnapshot() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	commandStats := make(map[string]map[string]interface{})

	for cmd, counter := range m.commandCount {
		errorCounter := m.commandErrorCount[cmd]
		latencyTracker := m.commandLatency[cmd]

		commandStats[cmd] = map[string]interface{}{
			"count":     counter.Load(),
			"errors":    errorCounter.Load(),
			"latencyMs": latencyTracker.Average(),
		}
	}

	return map[string]interface{}{
		"commands": commandStats,
		"cache": map[string]interface{}{
			"hits":     m.cacheHits.Load(),
			"misses":   m.cacheMisses.Load(),
			"hitRatio": m.GetCacheHitRatio(),
		},
	}
}

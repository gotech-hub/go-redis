package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message cấu trúc của một message trong hệ thống Pub/Sub
type Message struct {
	Channel  string      `json:"channel"`
	Topic    string      `json:"topic"`
	Data     interface{} `json:"data"`
	Metadata interface{} `json:"metadata,omitempty"`
	Time     time.Time   `json:"time"`
}

// MessageHandler hàm callback được gọi khi nhận được message từ channel
type MessageHandler func(ctx context.Context, msg *Message) error

// Publish gửi message tới một channel
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}

	start := time.Now()

	// Đóng gói message
	msg := &Message{
		Channel: channel,
		Data:    message,
		Time:    time.Now(),
	}

	// Serialize message thành JSON
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Publish message to Redis
	err = c.client.Publish(ctx, channel, data).Err()
	if c.metrics != nil {
		c.metrics.TrackCommand("Publish", start, err)
	}
	return err
}

// PublishWithMetadata gửi message kèm metadata tới một channel
func (c *Client) PublishWithMetadata(ctx context.Context, channel string, topic string, message interface{}, metadata interface{}) error {
	if c.client == nil {
		return errors.New("redis client is nil")
	}

	start := time.Now()

	// Đóng gói message
	msg := &Message{
		Channel:  channel,
		Topic:    topic,
		Data:     message,
		Metadata: metadata,
		Time:     time.Now(),
	}

	// Serialize message thành JSON
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Publish message to Redis
	err = c.client.Publish(ctx, channel, data).Err()
	if c.metrics != nil {
		c.metrics.TrackCommand("PublishWithMetadata", start, err)
	}
	return err
}

// Subscribe đăng ký nhận messages từ một hoặc nhiều channels
// Handler sẽ được gọi mỗi khi có message mới
// Hàm này chạy trong goroutine riêng và block cho đến khi bị cancel
func (c *Client) Subscribe(ctx context.Context, handler MessageHandler, channels ...string) (*redis.PubSub, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}

	if handler == nil {
		return nil, errors.New("message handler is nil")
	}

	if len(channels) == 0 {
		return nil, errors.New("no channels specified")
	}

	// Đăng ký nhận message từ channel(s)
	pubsub := c.client.Subscribe(ctx, channels...)

	// Chạy một goroutine để xử lý messages
	go func() {
		defer pubsub.Close()

		// Lắng nghe messages cho đến khi context bị cancel
		for {
			select {
			case <-ctx.Done():
				// Context bị cancel, thoát loop
				return
			default:
				// Tiếp tục xử lý
			}

			// Nhận message từ Redis
			redisMsg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				// Ghi log lỗi nhưng không dừng, trừ khi context bị cancel
				if ctx.Err() != nil {
					return
				}
				continue
			}

			// Parse message từ JSON
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				// Message không đúng định dạng, bỏ qua
				continue
			}

			// Gọi handler để xử lý message
			if err := handler(ctx, &msg); err != nil {
				// Ghi log lỗi nhưng không dừng
				continue
			}
		}
	}()

	return pubsub, nil
}

// SubscribeWithTopicFilter đăng ký nhận messages từ một channel và lọc theo topic
func (c *Client) SubscribeWithTopicFilter(ctx context.Context, channel string, topic string, handler MessageHandler) (*redis.PubSub, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}

	if handler == nil {
		return nil, errors.New("message handler is nil")
	}

	// Wrap handler với filter
	filteredHandler := func(ctx context.Context, msg *Message) error {
		// Chỉ xử lý message có topic phù hợp
		if msg.Topic == topic {
			return handler(ctx, msg)
		}
		return nil
	}

	// Sử dụng Subscribe thông thường với handler đã được wrap
	return c.Subscribe(ctx, filteredHandler, channel)
}

// SubscribeWithPattern đăng ký nhận messages từ channels khớp với pattern
func (c *Client) SubscribeWithPattern(ctx context.Context, pattern string, handler MessageHandler) (*redis.PubSub, error) {
	if c.client == nil {
		return nil, errors.New("redis client is nil")
	}

	if handler == nil {
		return nil, errors.New("message handler is nil")
	}

	// Đăng ký nhận message từ pattern
	pubsub := c.client.PSubscribe(ctx, pattern)

	// Chạy một goroutine để xử lý messages
	go func() {
		defer pubsub.Close()

		// Lắng nghe messages cho đến khi context bị cancel
		for {
			select {
			case <-ctx.Done():
				// Context bị cancel, thoát loop
				return
			default:
				// Tiếp tục xử lý
			}

			// Nhận message từ Redis
			redisMsg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				// Ghi log lỗi nhưng không dừng, trừ khi context bị cancel
				if ctx.Err() != nil {
					return
				}
				continue
			}

			// Parse message từ JSON
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				// Message không đúng định dạng, bỏ qua
				continue
			}

			// Gán channel thực tế vào message nếu cần
			msg.Channel = redisMsg.Channel

			// Gọi handler để xử lý message
			if err := handler(ctx, &msg); err != nil {
				// Ghi log lỗi nhưng không dừng
				continue
			}
		}
	}()

	return pubsub, nil
}

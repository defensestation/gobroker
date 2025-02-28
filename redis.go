// redis.go
// Implementation of Redis pub/sub for gobroker
package gobroker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis connection types
var (
	RedisPublishConnection  = "redis_publish"
	RedisConsumerConnection = "redis_consume"
)

// RedisConnection struct to maintain compatibility with Connection interface
type RedisConnection struct {
	*redis.Client
	Status      string
	Type        string
	ChannelPool map[int]*RedisChannel
	pickCounter int
}

// RedisChannel struct to maintain compatibility with Channel interface
type RedisChannel struct {
	Client *redis.Client
	Status string
	Id     int
	pubsub *redis.PubSub // For subscription channels
}

// Redis-specific options
type RedisOptions struct {
	DB       int
	Password string
}

// Add Redis connection to broker
func (e *Broker) AddRedisConnection(ctype string) (*RedisConnection, error) {
	// Parse Redis options from endpoint
	opts, err := redis.ParseURL(e.Endpoint)
	if err != nil {
		return nil, err
	}

	// Create Redis client
	client := redis.NewClient(opts)

	// Check connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	_, err = client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	// Create connection
	connection := &RedisConnection{
		Client:      client,
		Status:      "live",
		Type:        ctype,
		ChannelPool: map[int]*RedisChannel{},
		pickCounter: 1,
	}

	// Store connection
	e.connections[ctype] = connection

	// Start health check goroutine
	go func() {
	    ticker := time.NewTicker(30 * time.Second)
	    defer ticker.Stop()

	    for range ticker.C {
	        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	        _, err := client.Ping(ctx).Result()
	        cancel()

	        if err != nil {
	            connection.Status = "connection error"
	            log.Printf("Redis connection error: %v", err)

	            // Attempt to reconnect
	            for {
	                time.Sleep(time.Duration(delay) * time.Second)
	                
	                newClient := redis.NewClient(opts)
	                ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	                _, err := newClient.Ping(ctx).Result()
	                cancel()

	                if err == nil {
	                    connection.Client = newClient
	                    connection.Status = "live"
	                    log.Printf("Redis connection re-established")
	                    break
	                }

	                log.Printf("Redis reconnect failed: %v", err)
	                newClient.Close()
	            }
	        }
	    }
	}()


	return connection, nil
}

// Get Redis connection
func (e *Broker) GetRedisConnection(ctype string) (*RedisConnection, error) {
	// Check if connection exists
	if _, ok := e.connections[ctype]; ok {
		conn, ok := e.connections[ctype].(*RedisConnection)
		if !ok {
			return nil, errors.New("connection is not a Redis connection")
		}
		if conn.Status != "live" {
			return nil, errors.New("connection status not live")
		}
		return conn, nil
	}
	return e.AddRedisConnection(ctype)
}

// Add Redis channel
func (c *RedisConnection) AddRedisChannel() (*RedisChannel, error) {
	// Check connection status
	if c.Status != "live" {
		return nil, errors.New("connection not live")
	}

	// Pool length
	poolLength := len(c.ChannelPool)
	channelId := poolLength + 1

	// Create channel
	channel := &RedisChannel{
		Client: c.Client,
		Status: "live",
		Id:     channelId,
	}

	// Add to pool
	c.ChannelPool[channelId] = channel

	return channel, nil
}

// Get Redis channel
func (c *RedisConnection) GetRedisChannel(id ...int) (*RedisChannel, error) {
	// If id is provided return channel by that id
	if len(id) != 0 {
		_, ok := c.ChannelPool[id[0]]
		if ok {
			ch := c.ChannelPool[id[0]]
			if ch == nil {
				return nil, errors.New("unable to find channel")
			}
			if ch.Status != "live" {
				return nil, errors.New("channel status not live")
			}
			return ch, nil
		}
		return nil, errors.New("invalid id")
	}

	// Get pool length
	poolLength := len(c.ChannelPool)

	// Check if pool has any channels if not create and return
	if poolLength == 0 {
		return c.AddRedisChannel()
	}

	// Get channel
	ch := c.ChannelPool[c.pickCounter]
	if ch == nil {
		return nil, errors.New("unable to find channel")
	}
	
	// Check channel status
	if ch.Status != "live" {
		return nil, errors.New("channel status not live")
	}

	// Update pickcounter
	c.pickCounter = (c.pickCounter % poolLength) + 1

	return ch, nil
}

// Publish message to Redis
func (b *Broker) PublishToRedisChannel(channel string, body interface{}) error {
	// Get Redis publish connection
	conn, err := b.GetRedisConnection(RedisPublishConnection)
	if err != nil {
		return err
	}

	// Marshal the golang interface to json
	jsonString, err := json.Marshal(body)
	if err != nil {
		return err
	}

	// Publish message
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = conn.Publish(ctx, channel, jsonString).Err()
	if err != nil {
		return err
	}

	return nil
}

// Run Redis consumer
func (b *Broker) RunRedisConsumer(channels []string, handler func([]byte)) error {
	// Get Redis consumer connection
	conn, err := b.GetRedisConnection(RedisConsumerConnection)
	if err != nil {
		return err
	}

	// Create a new Redis channel
	ch, err := conn.AddRedisChannel()
	if err != nil {
		return err
	}

	// Create subscription
	pubsub := conn.Subscribe(context.Background(), channels...)
	ch.pubsub = pubsub

	// Start consumer goroutine
	go func() {
		defer pubsub.Close()

		// Process messages
		for {
			msg, err := pubsub.ReceiveMessage(context.Background())
			if err != nil {
				log.Printf("Redis subscription error: %v", err)
				ch.Status = "error"
				time.Sleep(time.Duration(delay) * time.Second)
				
				// Try to resubscribe
				pubsub = conn.Subscribe(context.Background(), channels...)
				ch.pubsub = pubsub
				ch.Status = "live"
				continue
			}

			// Process message
			handler([]byte(msg.Payload))
		}
	}()

	return nil
}

// Close Redis channel
func (ch *RedisChannel) Close() error {
	if ch.pubsub != nil {
		return ch.pubsub.Close()
	}
	return nil
}
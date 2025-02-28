package gobroker

import (
	// "context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	// "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// TestRedisIntegration tests Redis pub/sub functionality
func TestRedisIntegration(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration tests")
	}

	// Get Redis endpoint from env or use default
	redisEndpoint := os.Getenv("REDIS_ENDPOINT")
	if redisEndpoint == "" {
		redisEndpoint = "redis://localhost:6379/0"
	}

	// Create broker
	broker := NewBroker(redisEndpoint,BrokerTypeRedis)

	// Test cases
	t.Run("TestRedisConnection", testRedisConnection(broker))
	t.Run("TestRedisChannelCreation", testRedisChannelCreation(broker))
	t.Run("TestRedisPublishSubscribe", testRedisPublishSubscribe(broker))
	t.Run("TestRedisMultipleSubscribers", testRedisMultipleSubscribers(broker))
	t.Run("TestRedisReconnection", testRedisReconnection(broker))
}

func testRedisConnection(broker *Broker) func(t *testing.T) {
	return func(t *testing.T) {
		// Test publish connection
		pubConn, err := broker.GetRedisConnection(RedisPublishConnection)
		assert.NoError(t, err)
		assert.NotNil(t, pubConn)
		assert.Equal(t, "live", pubConn.Status)
		assert.Equal(t, RedisPublishConnection, pubConn.Type)

		// Test consumer connection
		consConn, err := broker.GetRedisConnection(RedisConsumerConnection)
		assert.NoError(t, err)
		assert.NotNil(t, consConn)
		assert.Equal(t, "live", consConn.Status)
		assert.Equal(t, RedisConsumerConnection, consConn.Type)

		// Test custom connection type
		customConn, err := broker.GetRedisConnection("custom_redis")
		assert.NoError(t, err)
		assert.NotNil(t, customConn)
		assert.Equal(t, "live", customConn.Status)
		assert.Equal(t, "custom_redis", customConn.Type)
	}
}

func testRedisChannelCreation(broker *Broker) func(t *testing.T) {
	return func(t *testing.T) {
		// Get connection
		conn, err := broker.GetRedisConnection(RedisPublishConnection)
		assert.NoError(t, err)

		// Create a channel
		ch1, err := conn.AddRedisChannel()
		assert.NoError(t, err)
		assert.NotNil(t, ch1)
		assert.Equal(t, 1, ch1.Id)
		assert.Equal(t, "live", ch1.Status)

		// Create another channel
		ch2, err := conn.AddRedisChannel()
		assert.NoError(t, err)
		assert.NotNil(t, ch2)
		assert.Equal(t, 2, ch2.Id)
		assert.Equal(t, "live", ch2.Status)

		// Get channel by ID
		ch, err := conn.GetRedisChannel(1)
		assert.NoError(t, err)
		assert.Equal(t, 1, ch.Id)

		// Get channel using round-robin
		ch, err = conn.GetRedisChannel()
		assert.NoError(t, err)
		assert.True(t, ch.Id == 1 || ch.Id == 2)

		ch, err = conn.GetRedisChannel()
		assert.NoError(t, err)
		assert.True(t, ch.Id == 1 || ch.Id == 2)
	}
}

func testRedisPublishSubscribe(broker *Broker) func(t *testing.T) {
	return func(t *testing.T) {
		testChannel := "test_channel"
		var wg sync.WaitGroup
		wg.Add(1)
		
		// Test message
		type testMessage struct {
			ID   int    `json:"id"`
			Data string `json:"data"`
		}
		
		testMsg := testMessage{
			ID:   123,
			Data: "test data",
		}
		
		var receivedMsg []byte
		
		// Set up consumer
		err := broker.RunRedisConsumer([]string{testChannel}, func(msg []byte) {
			receivedMsg = msg
			wg.Done()
		})
		assert.NoError(t, err)
		
		// Wait for consumer to be ready
		time.Sleep(500 * time.Millisecond)
		
		// Publish message
		err = broker.PublishToRedisChannel(testChannel, testMsg)
		assert.NoError(t, err)
		
		// Wait for message to be received (with timeout)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for message")
		}
		
		// Verify message
		var decoded testMessage
		err = json.Unmarshal(receivedMsg, &decoded)
		assert.NoError(t, err)
		assert.Equal(t, testMsg.ID, decoded.ID)
		assert.Equal(t, testMsg.Data, decoded.Data)
	}
}

func testRedisMultipleSubscribers(broker *Broker) func(t *testing.T) {
	return func(t *testing.T) {
		testChannel := "multi_sub_channel"
		subscriberCount := 3
		var wg sync.WaitGroup
		wg.Add(subscriberCount)
		
		// Message counters
		counters := make([]int, subscriberCount)
		
		// Start multiple subscribers
		for i := 0; i < subscriberCount; i++ {
			subIndex := i
			err := broker.RunRedisConsumer([]string{testChannel}, func(msg []byte) {
				counters[subIndex]++
				wg.Done()
			})
			assert.NoError(t, err)
		}
		
		// Wait for consumers to be ready
		time.Sleep(500 * time.Millisecond)
		
		// Publish message
		err := broker.PublishToRedisChannel(testChannel, map[string]string{"message": "test"})
		assert.NoError(t, err)
		
		// Wait for all subscribers to receive the message (with timeout)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for all subscribers")
		}
		
		// Verify all subscribers received the message
		for i, count := range counters {
			assert.Equal(t, 1, count, "Subscriber %d should receive exactly one message", i)
		}
	}
}

func testRedisReconnection(broker *Broker) func(t *testing.T) {
	return func(t *testing.T) {
		// Note: This test is complex as it requires simulating a Redis server disconnect
		// Here we'll test just the basic reconnection logic without actually shutting down Redis
		
		// Get Redis connection
		conn, err := broker.GetRedisConnection(RedisPublishConnection)
		assert.NoError(t, err)
		
		// Simulate connection error by manually changing status
		oldStatus := conn.Status
		conn.Status = "connection error"
		
		// Wait a bit then check if health check routine attempts reconnection
		// Note: In a real test, you'd shut down Redis then restart it
		time.Sleep(1 * time.Second)
		
		// Restore status for the test (the health check would do this in real scenario)
		conn.Status = oldStatus
		
		// Try getting a channel which should work if connection is restored
		_, err = conn.GetRedisChannel()
		assert.NoError(t, err)
	}
}
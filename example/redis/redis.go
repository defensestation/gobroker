package main

import (
	"context"
	"fmt"
	"github.com/defensestation/gobroker/v2"
	"log"
	"time"
)

func main() {

	// Example 2: Redis broker
	redisBroker := gobroker.NewBroker("localhost", gobroker.BrokerTypeRedis, &gobroker.EndpointOptions{
		Password: "",
		Port:     "6379",
		DB:       0,
	})
	defer redisBroker.Close()

	// Example message
	event := map[string]interface{}{
		"id":         12345,
		"type":       "user_registered",
		"username":   "johndoe",
		"email":      "john@example.com",
		"created_at": time.Now().Format(time.RFC3339),
		"source":     "web_app",
		"ip_address": "192.168.1.1",
		"user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"metadata": map[string]interface{}{
			"referrer": "google.com",
			"plan":     "premium",
		},
	}

	// From Redis
	err := redisBroker.Subscribe("user:events", func(data []byte) {
		fmt.Printf("Received Redis message: %s\n", string(data))
	})

	if err != nil {
		log.Printf("Failed to subscribe to Redis: %v", err)
	}

	// Example of using the unified API
	// To Redis
	err = redisBroker.Publish(context.TODO(), "user:events", event)
	if err != nil {
		log.Printf("Failed to publish to Redis: %v", err)
	}

	// Keep the program running to receive messages
	select {}
}

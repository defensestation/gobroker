package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/defensestation/gobroker/v2"
)

func main() {
	// Create Amazon MQ broker instance
	broker := gobroker.NewBroker("admin:password@mq-broker.example.com:61613", gobroker.BrokerTypeAmazonMQ)
	defer broker.Close()

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

	// Subscribe to a queue
	err := broker.Subscribe("/queue/user-events", func(data []byte) {
		// Parse the received data
		var receivedEvent map[string]interface{}
		if err := json.Unmarshal(data, &receivedEvent); err != nil {
			log.Printf("Error parsing message: %v", err)
			return
		}

		fmt.Printf("Received event: Type=%s, Username=%s\n",
			receivedEvent["type"], receivedEvent["username"])
	})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Publish a message
	err = broker.Publish(context.TODO(), "/queue/user-events", event)
	if err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}

	fmt.Println("Message published successfully")

	// Subscribe to a topic
	err = broker.Subscribe("/topic/notifications", func(data []byte) {
		fmt.Printf("Received notification: %s\n", string(data))
	})

	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	// Publish to a topic
	notification := map[string]interface{}{
		"message":  "System maintenance scheduled",
		"time":     "2023-05-01T22:00:00Z",
		"severity": "info",
	}

	err = broker.Publish(context.TODO(), "/topic/notifications", notification)
	if err != nil {
		log.Fatalf("Failed to publish notification: %v", err)
	}

	fmt.Println("Notification published successfully")

	// Keep the application running to receive messages
	select {}
}

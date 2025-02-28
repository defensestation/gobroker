package main

import (
	"fmt"
	"log"
	"time"

	"github.com/defensestation/gobroker"
)

func main() {

	// Example 2: Redis broker
	redisBroker := gobroker.NewBroker("localhost", gobroker.BrokerTypeRedis, &gobroker.EndpointOptions{
		Password: "",
		Port:     "6379",
		DB:       0,
	})
	defer redisBroker.Close()

	// Example of using the unified API
	// To Redis
	err = redisBroker.Publish("user:events", userEvent)
	if err != nil {
		log.Printf("Failed to publish to Redis: %v", err)
	}

	// From Redis
	err = redisBroker.Subscribe("user:events", func(data []byte) {
		fmt.Printf("Received Redis message: %s\n", string(data))
	})
	
	if err != nil {
		log.Printf("Failed to subscribe to Redis: %v", err)
	}
	
	// Keep the program running to receive messages
	select {}
}
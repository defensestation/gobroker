package main

import (
	"fmt"
	"time"
	json "encoding/json"
	"github.com/defensestation/go-broker"
)

var (
	exchangeName = "demo-service"
	routeKey 	 = "demo.event.test"
	message      = map[string]string{"msg": "test"}
)

func main() {

	// create broker
	// endpoint does not require to add protocol 
	// endpoint options can be provided: &EndpointOptions{Username: "guest", Password: "guest", Port: "5672"}
	newbroker := broker.NewBroker("172.18.0.2", &broker.EndpointOptions{Username: "guest", Password: "guest", Port: "5672"})
	
	// build exchange
	ex, err := newbroker.BuildExchange(exchangeName)
	if err != nil {
		fmt.Println(err)
		return
	}

	// start consumer
	err = ex.RunConsumer(exchangeName, routeKey, ConsumeMethod, "")
	if err != nil {
		fmt.Println(err)
		return
	}

	// publish message to queue
	err = ex.Publish(routeKey, message)
	if err != nil {
		fmt.Println(err)
		return
	}	
	// // wait 1s
	time.Sleep(time.Duration(1) * time.Second)
}

// consume method
func ConsumeMethod(message []byte) {
	response := make(map[string]string)
	json.Unmarshal(message, &response) 

	fmt.Println("Message Recived:%v", response)
}
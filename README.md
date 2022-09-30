# Golang Broker Package

This package provides a abstraction layer on top of brokers like rabbitmq, amazon mq and make publishing and consuming messages eaiser. Main use-case for this package is micro-service architecture

## Usage

- Setup Broker
```
newbroker := broker.NewBroker("amqp://guest:guest@127.0.0.1:5672/")
```

- Build Exchange
```
ex, err := newbroker.BuildExchange("exchange-name")
if err != nil {
	...
}
```

- Publish Message
```
err = ex.Publish(
		"servive.event.type", //route key
		map[string]string{"msg": "test"} //message, type: map[string]interface{}
		)
if err != nil {
	...
}	
```

- Start Consumer
```
err = ex.RunConsumer(
		"exchange_name", 
		"service.event.type", //route key
		ConsumeMethod, //customerMethodName
		"" //queue name, leave empty for exclusive queue
		)
if err != nil {
	...
}
```

- Consume Method
```
func ConsumeMethod(message []byte) {
	response := make(map[string]string)
	json.Unmarshal(message, &response) 

	fmt.Println("Message Recived:%v", response)
}
```

## Example
Full example: [link](example/)
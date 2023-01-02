# Golang Broker Package

This package provides an abstraction layer on top of brokers like RabbitMQ, AmazonMQ and make publishing and consuming messages easier while following best practices. Main use-case for this package is micro-service architecture.

## Features
- Separate connection for publishing and consuming
- Connection and Channel pooling

### Supported Broker
	- RabbitMQ

## Usage

- Setup Broker
```
newbroker := broker.NewBroker("127.0.0.1", &broker.EndpointOptions{
						Username: "guest", 
						Password: "guest", 
						Port: "5672"
					})
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
		"servive.event.type", 			//route key
		map[string]string{"msg": "test"} 	//message, type: map[string]interface{}
		)
if err != nil {
	...
}	
```

- Start Consumer
```
err = ex.RunConsumer(
		"exchange_name", 
		"service.event.type", 	//route key
		ConsumeMethod, 		//customerMethodName
		"" 			//queue name, leave empty for exclusive queue
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

	fmt.Println("Message Recieved:%v", response)
}
```

## Upcoming Features
- AmazonMQ Support

## Example
[Full example](example/)

## Contributing
We welcome contributions to gobroker. If you would like to report a bug or request a new feature, please open an issue on GitHub. If you would like to contribute code, please submit a pull request.

## License
GoBroker is licensed under the (MIT License)[license.md].
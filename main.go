// version: 0.0.1
// broker wrapper is lib to manage creation of exchanges and consumers
// only type supported is rabbitmq

package broker

import  (
	amqp "github.com/streadway/amqp"
)

// broker struct
type Broker struct {
	Endpoint 	string
	Type 		string	// only rabbitmq supported
}

// new broker
func NewBroker(endpoint string) (*Broker, error) {
	// check type of broker if multiple supported
	broker := &Broker{
		Endpoint: endpoint,
		Servicename: servicename,
		Type: "rabbitmq",
	}

	// create tcp cpnnection
	err := broker.connect()
	if err != nil {
		return nil, err
	}

	return broker, nil
}
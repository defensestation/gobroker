// version: 0.0.1
// broker wrapper is lib to manage creation of exchanges and consumers
// only type supported is rabbitmq

package broker

// broker struct
type Broker struct {
	Endpoint 	string
	Type 		string	// only rabbitmq supported
}

var delay = 5 // reconnet delay 5 seconds

// new broker
func NewBroker(endpoint string) (*Broker) {
	// check type of broker if multiple supported
	return &Broker{
		Endpoint: endpoint,
		Type: "rabbitmq",
	}
}
// version: 0.0.1
// broker wrapper is lib to manage creation of exchanges and consumers
// only type supported is rabbitmq
package broker

import (
	"fmt"
)

var delay = 5 // reconnet delay 5 seconds

// broker struct
type Broker struct {
	Endpoint string
	Type     string // only rabbitmq supported
}

// broker options like username password
type EndpointOptions struct {
	Protocol string
	Username string
	Password string
	Port     string
}

// new broker
func NewBroker(endpoint string, opts ...*EndpointOptions) *Broker {
	// check broker options are provided, update endpoint
	if len(opts) != 0 {
		options := opts[0]

		// set defaults
		if options.Protocol == "" {
			options.Protocol = "amqp"
		}
		if options.Port == "" {
			options.Port = "5671"
		}

		endpoint = fmt.Sprintf("%s://%s:%s@%s", options.Protocol, options.Username, options.Password, endpoint)
		// check if port is provided
		endpoint = fmt.Sprintf("%s:%s/", endpoint, options.Port)
	} else {
		// append protocol to endpoint
		endpoint = endpoint
	}

	// check type of broker if multiple supported
	return &Broker{
		Endpoint: endpoint,
		Type:     "rabbitmq",
	}
}

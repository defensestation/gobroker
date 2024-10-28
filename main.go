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
	connections map[string]*Connection
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
		connections: map[string]*Connection{},
	}
}


// only declare and bind
func (b *Broker) QueueDeclareAndBind(exchange, routeKey, queueName string) (string, error) {
	
	conn, err := b.GetConnection(ConsumerConnection)
	if err != nil {
		return "", err
	}

	// user consumer connection and add new channel for this routine
	ch, err := conn.AddChannel()
	// check if any errors
	if err != nil {
		return "", err
	}

	// declare queue
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		// usally when the qeueu exist only between service to broker name is not defined
		// then it's a exclusive queue
		(queueName == ""), // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	// check if any error
	if err != nil {
		return "", err
	}

	// bind queue to echange
	err = ch.QueueBind(
		q.Name,   // queue name
		routeKey, // routing key
		exchange, // exchange
		false,    // no-wait
		nil,      // arguments
	)
	// check if any errors
	if err != nil {
		return "", err
	}

	return q.Name, nil
}

// only one channel is used per go cosumer
func (b *Broker) RunConsumer(exchange, routeKey string, functions func([]byte), queueName string) error {
	// get connection
	conn, err := b.GetConnection(ConsumerConnection)
	if err != nil {
		return err
	}

	// user consumer connection and add new channel for this routine
	ch, err := conn.AddChannel()
	// check if any errors
	if err != nil {
		return err
	}

	qName, err := b.QueueDeclareAndBind(exchange, routeKey, queueName, ch)
	if err != nil {
		return err
	}

	// build consumer
	msgs, err := ch.Consume(
		qName, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	// check if any errors
	if err != nil {
		return err
	}

	// start consumer connection and send every message to functoion
	go func() {
		// get the same channel in go routine
		ch, _ := b.connections[ConsumerConnection].GetChannel(ch.Id)
		for d := range msgs {
			functions(d.Body)
		}
		// close the channel with go routine ends
		defer ch.Close()
	}()

	return nil
}
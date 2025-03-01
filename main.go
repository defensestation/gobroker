// version: 0.0.1
// broker wrapper is lib to manage creation of exchanges and consumers
// only type supported is rabbitmq
package gobroker

import (
	"context"
	"fmt"
	"log"
	"crypto/tls"
	"strings"
)

// Define the reconnect delay
var delay = 5 // 5 second delay for reconnection attempts

// Updated broker type constants
const (
	BrokerTypeRabbitMQ = "rabbitmq"
	BrokerTypeRedis    = "redis"
	BrokerTypeAmazonMQ = "amazonmq"
)

// EndpointOptions defines connection parameters for different broker types
type EndpointOptions struct {
	Protocol string
	Username string
	Password string
	Port     string
	DB       int // For Redis
	TLSConfig *tls.Config // TLS configuration for secure connection
}

// Broker represents a message broker connection
type Broker struct {
	opts *EndpointOptions
	Endpoint    string
	Type        string
	connections map[string]interface{}
}

// NewBroker creates a new broker with unified API
func NewBroker(endpoint string, brokerType string, opts ...*EndpointOptions) *Broker {
	var formattedEndpoint string

	// Check broker options are provided, update endpoint
	if len(opts) != 0 {
		options := opts[0]

		switch brokerType {
		case BrokerTypeRabbitMQ:
			// Set defaults for RabbitMQ
			if options.Protocol == "" {
				options.Protocol = "amqp"
			}
			if options.Port == "" {
				options.Port = "5671"
			}

			formattedEndpoint = fmt.Sprintf("%s://%s:%s@%s:%s/",
				options.Protocol, options.Username, options.Password, endpoint, options.Port)

		case BrokerTypeRedis:
			// Set defaults for Redis
			if options.Protocol == "" {
				options.Protocol = "redis"
			}
			if options.Port == "" {
				options.Port = "6379"
			}

			// If TLS configuration is provided, change protocol to secure redis (rediss)
			if options.TLSConfig != nil {
				options.Protocol = "rediss"
			}

			// Format Redis URL
			auth := ""
			if options.Password != "" {
				if options.Username != "" {
					auth = options.Username + ":" + options.Password + "@"
				} else {
					auth = ":" + options.Password + "@"
				}
			}

			dbSuffix := ""
			if options.DB > 0 {
				dbSuffix = fmt.Sprintf("/%d", options.DB)
			}

			formattedEndpoint = fmt.Sprintf("%s://%s%s:%s%s",
				options.Protocol, auth, endpoint, options.Port, dbSuffix)

		case BrokerTypeAmazonMQ:
			// For AmazonMQ, format should be username:password@host:port
			if options.Port == "" {
				options.Port = "61613" // Default STOMP port
			}

			formattedEndpoint = fmt.Sprintf("%s:%s@%s:%s",
				options.Username, options.Password, endpoint, options.Port)
		}
	} else {
		// Use endpoint as-is if no options provided
		formattedEndpoint = endpoint
		opts = []*EndpointOptions{}
	}

	return &Broker{
		opts: opts[0],
		Endpoint:    formattedEndpoint,
		Type:        brokerType,
		connections: map[string]interface{}{},
	}
}

// Extended unified publish method
func (b *Broker) Publish(ctx context.Context, topic string, body interface{}) error {
	switch b.Type {
	case BrokerTypeRabbitMQ:
		// For RabbitMQ, topic should be in format "exchange.routekey"
		parts := strings.SplitN(topic, ".", 2)
		if len(parts) < 2 {
			return fmt.Errorf("invalid topic format for RabbitMQ, should be 'exchange.routekey'")
		}
		exchange, routeKey := parts[0], parts[1]
		return b.PublishToExchange(ctx, exchange, routeKey, body)

	case BrokerTypeRedis:
		// For Redis, topic is the channel name
		return b.PublishToRedisChannel(topic, body)

	case BrokerTypeAmazonMQ:
		// For Amazon MQ, topic is the destination queue/topic
		return b.PublishToAmazonMQQueue(topic, body)

	default:
		return fmt.Errorf("unsupported broker type: %s", b.Type)
	}
}

// Extended unified subscribe method
func (b *Broker) Subscribe(topic string, handler func([]byte), queueName ...string) error {
	switch b.Type {
	case BrokerTypeRabbitMQ:
		// For RabbitMQ, topic should be in format "exchange.routekey"
		parts := strings.SplitN(topic, ".", 2)
		if len(parts) < 2 {
			return fmt.Errorf("invalid topic format for RabbitMQ, should be 'exchange.routekey'")
		}
		exchange, routeKey := parts[0], parts[1]

		// Use provided queue name or empty string for auto-generated queue
		queue := ""
		if len(queueName) > 0 {
			queue = queueName[0]
		}

		return b.RunConsumer(exchange, routeKey, handler, queue)

	case BrokerTypeRedis:
		// For Redis, topic is the channel name
		return b.RunRedisConsumer([]string{topic}, handler)

	case BrokerTypeAmazonMQ:
		// For Amazon MQ, topic is the destination queue/topic
		return b.RunAmazonMQConsumer(topic, handler)

	default:
		return fmt.Errorf("unsupported broker type: %s", b.Type)
	}
}

// Close connections for all broker types
func (b *Broker) Close() {
	for _, conn := range b.connections {
		switch c := conn.(type) {
		case *Connection:
			if c.Connection != nil {
				c.Close()
			}
		case *RedisConnection:
			if c.Client != nil {
				c.Close()
			}
		case *AmazonMQConnection:
			if c.Conn != nil {
				err := c.Conn.Disconnect()
				if err != nil {
					log.Printf("failed to close *AmazonMQConnection connection")
					return
				}
			}
		}
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

	// Declare and bind queue, getting the actual queue name
	q, err := b.QueueDeclareAndBind(exchange, routeKey, queueName)
	if err != nil {
		return err
	}

	// build consumer using the queue name from QueueDeclareAndBind
	msgs, err := ch.Consume(
		q,     // queue
		"",    // consumer
		true,  // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)
	// check if any errors
	if err != nil {
		return err
	}

	// start consumer connection and send every message to functoion
	go func() {
		// get the same channel in go routine
		ch, _ := b.connections[ConsumerConnection].(*Connection).GetChannel(ch.Id)
		for d := range msgs {
			functions(d.Body)
		}
		// close the channel with go routine ends
		defer ch.Close()
	}()

	return nil
}

// amazon_mq.go
// Implementation of Amazon MQ for gobroker (using STOMP protocol)

package gobroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"log"
	"time"
)

var (
	AmazonMQPublishConnection  = "amazonmq_publish"
	AmazonMQConsumerConnection = "amazonmq_consume"
)

// AmazonMQConnection struct
type AmazonMQConnection struct {
	// The underlying STOMP connection
	Conn        *stomp.Conn
	Status      string
	Type        string
	ChannelPool map[int]*AmazonMQChannel
	pickCounter int

	// We'll just store the address or any needed info here
	Address string
	// If you want to store the original stomp options:
	Options []func(*stomp.Conn) error
}

// AmazonMQChannel struct
type AmazonMQChannel struct {
	Conn   *stomp.Conn
	Status string
	Id     int

	// Subscription reference, if this channel is used by a consumer
	Sub *stomp.Subscription
}

// AddAmazonMQConnection: create a new STOMP connection and attach it to Broker
func (e *Broker) AddAmazonMQConnection(ctype string) (*AmazonMQConnection, error) {
	username, password, address, err := parseAmazonMQEndpoint(e.Endpoint)
	if err != nil {
		return nil, err
	}

	// Build the stomp options you want
	options := []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(username, password),
		// Example: 30s heartbeat each way
		stomp.ConnOpt.HeartBeat(30*time.Second, 30*time.Second),
		// Add more if needed
	}

	// Create STOMP connection
	conn, err := stomp.Dial("tcp", address, options...)
	if err != nil {
		return nil, err
	}

	connection := &AmazonMQConnection{
		Conn:        conn,
		Status:      "live",
		Type:        ctype,
		ChannelPool: make(map[int]*AmazonMQChannel),
		pickCounter: 1,
		Address:     address,
		Options:     options, // we can store these if we want to reuse them for reconnection
	}

	if e.connections == nil {
		e.connections = make(map[string]interface{})
	}
	e.connections[ctype] = connection

	// Health-check / reconnection goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			<-ticker.C

			// If connection or a "ping" fails, attempt reconnect
			if conn == nil {
				connection.Status = "connection error"
				log.Printf("Amazon MQ connection is nil, attempting reconnect")
				reconnectAmazonMQConnection(connection)
				continue
			}

			// "Ping" test
			pingErr := conn.Send("/ping", "text/plain", []byte("ping"))
			if pingErr != nil {
				connection.Status = "connection error"
				log.Printf("Amazon MQ connection error: %v", pingErr)
				reconnectAmazonMQConnection(connection)
			}
		}
	}()

	return connection, nil
}

// Helper to handle reconnection attempts
func reconnectAmazonMQConnection(conn *AmazonMQConnection) {
	for {
		time.Sleep(time.Duration(delay) * time.Second)

		newConn, err := stomp.Dial("tcp", conn.Address, conn.Options...)
		if err == nil {
			conn.Conn = newConn
			conn.Status = "live"
			log.Printf("Amazon MQ connection re-established to %s", conn.Address)
			return
		}

		log.Printf("Amazon MQ reconnect failed: %v", err)
	}
}

// parseAmazonMQEndpoint: simplistic parser for "username:password@host:port"
func parseAmazonMQEndpoint(endpoint string) (username, password, address string, err error) {
	var host, port string
	_, err = fmt.Sscanf(endpoint, "%s:%s@%s:%s", &username, &password, &host, &port)
	if err != nil {
		return "", "", "", errors.New("invalid Amazon MQ endpoint format, should be username:password@host:port")
	}

	address = fmt.Sprintf("%s:%s", host, port)
	return username, password, address, nil
}

// GetAmazonMQConnection: retrieves an existing connection or creates one if needed
func (e *Broker) GetAmazonMQConnection(ctype string) (*AmazonMQConnection, error) {
	if e.connections == nil {
		e.connections = make(map[string]interface{})
	}

	if raw, ok := e.connections[ctype]; ok {
		if conn, ok := raw.(*AmazonMQConnection); ok {
			if conn.Status != "live" {
				return nil, errors.New("connection status not live")
			}
			return conn, nil
		}
		return nil, errors.New("connection is not an Amazon MQ connection")
	}

	// Not found, create a new one
	return e.AddAmazonMQConnection(ctype)
}

// AddAmazonMQChannel
func (c *AmazonMQConnection) AddAmazonMQChannel() (*AmazonMQChannel, error) {
	if c.Status != "live" {
		return nil, errors.New("connection not live")
	}

	channelId := len(c.ChannelPool) + 1
	channel := &AmazonMQChannel{
		Conn:   c.Conn,
		Status: "live",
		Id:     channelId,
		Sub:    nil,
	}

	c.ChannelPool[channelId] = channel
	return channel, nil
}

// GetAmazonMQChannel
func (c *AmazonMQConnection) GetAmazonMQChannel(id ...int) (*AmazonMQChannel, error) {
	if len(id) > 0 {
		ch, ok := c.ChannelPool[id[0]]
		if !ok || ch == nil {
			return nil, errors.New("unable to find channel")
		}
		if ch.Status != "live" {
			return nil, errors.New("channel status not live")
		}
		return ch, nil
	}

	// Round-robin selection if no ID is given
	poolSize := len(c.ChannelPool)
	if poolSize == 0 {
		return c.AddAmazonMQChannel()
	}

	ch, ok := c.ChannelPool[c.pickCounter]
	if !ok || ch == nil {
		return nil, errors.New("unable to find channel")
	}
	if ch.Status != "live" {
		return nil, errors.New("channel status not live")
	}

	c.pickCounter = (c.pickCounter % poolSize) + 1
	return ch, nil
}

// PublishToAmazonMQQueue publishes a message (JSON-encoded) to a STOMP destination
func (b *Broker) PublishToAmazonMQQueue(destination string, body interface{}) error {
	// Acquire the publish connection
	conn, err := b.GetAmazonMQConnection(AmazonMQPublishConnection)
	if err != nil {
		return err
	}

	// Acquire a channel
	ch, err := conn.GetAmazonMQChannel()
	if err != nil {
		return err
	}

	// Encode as JSON
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	// Provide optional headers (e.g., content-type)
	headers := []func(*frame.Frame) error{
		stomp.SendOpt.Header("content-type", "application/json"),
		// Additional headers if needed
	}

	// Send the message
	return ch.Conn.Send(destination, "application/json", payload, headers...)
}

// RunAmazonMQConsumer subscribes to a STOMP destination and processes messages
func (b *Broker) RunAmazonMQConsumer(destination string, handler func([]byte)) error {
	// Acquire the consumer connection
	conn, err := b.GetAmazonMQConnection(AmazonMQConsumerConnection)
	if err != nil {
		return err
	}

	// Acquire a channel
	ch, err := conn.GetAmazonMQChannel()
	if err != nil {
		return err
	}

	// Subscription options (headers), if any
	subOpts := []func(*frame.Frame) error{
		// For example: stomp.SubscribeOpt.Header("selector", "some-value"),
	}

	// Subscribe with an AckMode (e.g., AckAuto)
	sub, err := ch.Conn.Subscribe(destination, stomp.AckAuto, subOpts...)
	if err != nil {
		return err
	}
	ch.Sub = sub

	// Start a consumer loop
	go func() {
		for {
			msg := <-sub.C
			if msg == nil {
				log.Printf("Amazon MQ subscription channel closed.")
				ch.Status = "error"

				// Attempt to re-subscribe after a delay
				time.Sleep(time.Duration(delay) * time.Second)

				// Quick check: if the entire connection is down, wait for reconnection
				pingErr := conn.Conn.Send("/ping", "text/plain", []byte("ping"))
				if pingErr != nil {
					// Wait until the health-check goroutine reconnects
					for conn.Status != "live" {
						time.Sleep(time.Second)
					}
				}

				// Try again
				newSub, err := conn.Conn.Subscribe(destination, stomp.AckAuto, subOpts...)
				if err != nil {
					log.Printf("Failed to resubscribe: %v", err)
					continue
				}

				ch.Sub = newSub
				ch.Status = "live"
				sub = newSub
				continue
			}

			if msg.Err != nil {
				log.Printf("Amazon MQ message error: %v", msg.Err)
				continue
			}

			// Normal message => run your handler
			handler(msg.Body)
		}
	}()

	return nil
}

// Close an AmazonMQChannel
func (ch *AmazonMQChannel) Close() error {
	if ch.Sub != nil {
		return ch.Sub.Unsubscribe()
	}
	return nil
}

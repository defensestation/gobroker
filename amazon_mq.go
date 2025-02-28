// amazonmq.go
// Implementation of Amazon MQ for gobroker (using STOMP protocol)
package gobroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-stomp/stomp/v3"
)

// Amazon MQ connection types
var (
	AmazonMQPublishConnection  = "amazonmq_publish"
	AmazonMQConsumerConnection = "amazonmq_consume"
)

// AmazonMQConnection struct to maintain compatibility with Connection interface
type AmazonMQConnection struct {
	*stomp.Conn
	Status      string
	Type        string
	ChannelPool map[int]*AmazonMQChannel
	pickCounter int
	Options     *stomp.ConnOptions
	Address     string
}

// AmazonMQChannel struct to maintain compatibility with Channel interface
type AmazonMQChannel struct {
	Conn   *stomp.Conn
	Status string
	Id     int
	Sub    *stomp.Subscription // For subscription channels
}

// AmazonMQ-specific options
type AmazonMQOptions struct {
	HeartBeat time.Duration
	Protocol  string // Supports "stomp", default is "stomp" (others could be added like "amqp")
}

// Add AmazonMQ connection to broker
func (e *Broker) AddAmazonMQConnection(ctype string) (*AmazonMQConnection, error) {
	// Parse options from endpoint - assuming format "username:password@host:port"
	// Extract username, password, host, port
	username, password, address, err := parseAmazonMQEndpoint(e.Endpoint)
	if err != nil {
		return nil, err
	}

	// Set connection options
	options := &stomp.ConnOptions{
		Login:    username,
		Passcode: password,
		// Default heartbeat
		HeartBeat: time.Second * 30,
	}

	// Create STOMP connection
	conn, err := stomp.Dial("tcp", address, options)
	if err != nil {
		return nil, err
	}

	// Create connection
	connection := &AmazonMQConnection{
		Conn:        conn,
		Status:      "live",
		Type:        ctype,
		ChannelPool: map[int]*AmazonMQChannel{},
		pickCounter: 1,
		Options:     options,
		Address:     address,
	}

	// Store connection
	e.connections[ctype] = connection

	// Fixed health check goroutine for AmazonMQ
	go func() {
	    ticker := time.NewTicker(30 * time.Second)
	    defer ticker.Stop()

	    for {
	        select {
	        case <-ticker.C:
	            // Check if connection is still alive by sending a heartbeat
	            if connection.Conn == nil || !connection.Conn.Connected() {
	                connection.Status = "connection error"
	                log.Printf("Amazon MQ connection error")

	                // Attempt to reconnect
	                for {
	                    reconnectDelay := time.Duration(delay) * time.Second // Using 5 seconds as default delay
	                    time.Sleep(reconnectDelay)
	                    
	                    newConn, err := stomp.Dial("tcp", address, options)
	                    if err == nil {
	                        connection.Conn = newConn
	                        connection.Status = "live"
	                        log.Printf("Amazon MQ connection re-established")
	                        break
	                    }

	                    log.Printf("Amazon MQ reconnect failed: %v", err)
	                }
	            }
	        }
	    }
	}()

	return connection, nil
}

// Helper to parse Amazon MQ endpoint
func parseAmazonMQEndpoint(endpoint string) (username, password, address string, err error) {
	// Simple parsing - you might want to use a URL parser in production
	// Format: username:password@host:port
	var host, port string
	
	// Extract auth info
	_, err = fmt.Sscanf(endpoint, "%s:%s@%s:%s", &username, &password, &host, &port)
	if err != nil {
		return "", "", "", errors.New("invalid Amazon MQ endpoint format, should be username:password@host:port")
	}
	
	address = fmt.Sprintf("%s:%s", host, port)
	return username, password, address, nil
}

// Get AmazonMQ connection
func (e *Broker) GetAmazonMQConnection(ctype string) (*AmazonMQConnection, error) {
	// Check if connection exists
	if _, ok := e.connections[ctype]; ok {
		conn, ok := e.connections[ctype].(*AmazonMQConnection)
		if !ok {
			return nil, errors.New("connection is not an Amazon MQ connection")
		}
		if conn.Status != "live" {
			return nil, errors.New("connection status not live")
		}
		return conn, nil
	}
	return e.AddAmazonMQConnection(ctype)
}

// Add AmazonMQ channel
func (c *AmazonMQConnection) AddAmazonMQChannel() (*AmazonMQChannel, error) {
	// Check connection status
	if c.Status != "live" {
		return nil, errors.New("connection not live")
	}

	// Pool length
	poolLength := len(c.ChannelPool)
	channelId := poolLength + 1

	// Create channel
	channel := &AmazonMQChannel{
		Conn:   c.Conn,
		Status: "live",
		Id:     channelId,
	}

	// Add to pool
	c.ChannelPool[channelId] = channel

	return channel, nil
}

// Get AmazonMQ channel
func (c *AmazonMQConnection) GetAmazonMQChannel(id ...int) (*AmazonMQChannel, error) {
	// If id is provided return channel by that id
	if len(id) != 0 {
		_, ok := c.ChannelPool[id[0]]
		if ok {
			ch := c.ChannelPool[id[0]]
			if ch == nil {
				return nil, errors.New("unable to find channel")
			}
			if ch.Status != "live" {
				return nil, errors.New("channel status not live")
			}
			return ch, nil
		}
		return nil, errors.New("invalid id")
	}

	// Get pool length
	poolLength := len(c.ChannelPool)

	// Check if pool has any channels if not create and return
	if poolLength == 0 {
		return c.AddAmazonMQChannel()
	}

	// Get channel
	ch := c.ChannelPool[c.pickCounter]
	if ch == nil {
		return nil, errors.New("unable to find channel")
	}
	
	// Check channel status
	if ch.Status != "live" {
		return nil, errors.New("channel status not live")
	}

	// Update pickcounter
	c.pickCounter = (c.pickCounter % poolLength) + 1

	return ch, nil
}

// Publish message to Amazon MQ
func (b *Broker) PublishToAmazonMQQueue(destination string, body interface{}) error {
	// Get Amazon MQ publish connection
	conn, err := b.GetAmazonMQConnection(AmazonMQPublishConnection)
	if err != nil {
		return err
	}

	// Get channel
	ch, err := conn.GetAmazonMQChannel()
	if err != nil {
		return err
	}

	// Marshal the golang interface to json
	jsonString, err := json.Marshal(body)
	if err != nil {
		return err
	}

	// Set message options
	opts := &stomp.SendOptions{
		ContentType: "application/json",
	}

	// Publish message
	err = ch.Conn.Send(destination, "application/json", jsonString, opts)
	if err != nil {
		return err
	}

	return nil
}

// Run Amazon MQ consumer
func (b *Broker) RunAmazonMQConsumer(destination string, handler func([]byte)) error {
	// Get Amazon MQ consumer connection
	conn, err := b.GetAmazonMQConnection(AmazonMQConsumerConnection)
	if err != nil {
		return err
	}

	// Create a new Amazon MQ channel
	ch, err := conn.GetAmazonMQChannel()
	if err != nil {
		return err
	}

	// Set subscription options
	opts := &stomp.SubscribeOptions{
		Ack: stomp.AckAuto, // Auto acknowledge messages
	}

	// Create subscription
	sub, err := ch.Conn.Subscribe(destination, stomp.AckAuto, opts)
	if err != nil {
		return err
	}
	ch.Sub = sub

	// Start consumer goroutine
	go func() {
		for {
			msg := <-sub.C
			if msg == nil {
				log.Printf("Amazon MQ subscription channel closed")
				ch.Status = "error"
				
				// Try to resubscribe
				time.Sleep(time.Duration(delay) * time.Second)
				
				// Check if we need to reconnect
				if !conn.Conn.Connected() {
					// Wait for the reconnection logic to handle reconnecting
					for conn.Status != "live" {
						time.Sleep(time.Second)
					}
				}
				
				newSub, err := conn.Conn.Subscribe(destination, stomp.AckAuto, opts)
				if err != nil {
					log.Printf("Failed to resubscribe: %v", err)
					continue
				}
				
				ch.Sub = newSub
				ch.Status = "live"
				sub = newSub
				continue
			}
			
			// Process message
			handler(msg.Body)
		}
	}()

	return nil
}

// Close AmazonMQ channel
func (ch *AmazonMQChannel) Close() error {
	if ch.Sub != nil {
		return ch.Sub.Unsubscribe()
	}
	return nil
}
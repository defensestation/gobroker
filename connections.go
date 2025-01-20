// version: 0.0.1
// file to manage connection
package gobroker

import (
	errors "errors"
	log "log"
	time "time"
	net "net"
	fmt "fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// connection types
var (
	PublishConnection  = "publish"
	ConsumerConnection = "consume"
)

type Connection struct {
	*amqp.Connection
	Status      string
	Type        string
	ChannelPool map[int]*Channel
	pickCounter int
}

func resolveAndDial(endpoint string) (*amqp.Connection, error) {
	// Resolve the hostname manually
	resolver := net.Resolver{}
	ips, err := resolver.LookupHost(context.Background(), endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve hostname %s: %v", endpoint, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no IPs found for hostname %s", endpoint)
	}

	// Use the first IP for connection
	dialEndpoint := fmt.Sprintf("amqps://%s", ips[0])
	log.Printf("Resolved %s to %s, dialing...", endpoint, dialEndpoint)
	return amqp.Dial(dialEndpoint)
}

// create tls connection to borker
func (e *Broker) AddConnection(ctype string) (*Connection, error) {
	// create the dial
	connection, err := resolveAndDial(e.Endpoint)
	if err != nil {
		return nil, err
	}

	// set connection
	e.connections[ctype] = &Connection{
		connection,
		"live",
		ctype,
		map[int]*Channel{},
		1,
	}

	// start go routine that listen for connection close
	go func() {
		// Listen to NotifyClose
		for {
			reason, ok := <-connection.NotifyClose(make(chan *amqp.Error))
			if reason == nil {
				reason = &amqp.Error{
					Code: 1337,
					Reason: "Unknown: got the notifyclose reason as nil",
				}
			}
			// reset channels and set channel status to dead
			e.connections[ctype] = &Connection{
				Status:      reason.Reason,
				ChannelPool: map[int]*Channel{},
				pickCounter: 1,
			}
			// exit this goroutine if closed by developer
			if !ok {
				delete(e.connections, ctype)
				log.Printf("-> connection is closed by developer")
				break
			}
			log.Printf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				resolveAndDial(e.Endpoint)
				if err == nil {
					// set connection
					e.connections[ctype] = &Connection{
						connection,
						"live",
						ctype,
						map[int]*Channel{},
						1,
					}
					break
				}

				log.Printf("reconnect failed, err: %v", err)
			}
		}
	}()

	return e.connections[ctype], nil
}

func (e *Broker) GetConnection(ctype string) (*Connection, error) {
	// check if connection exists
	if _, ok := e.connections[ctype]; ok {
		conn := e.connections[ctype]
		if conn.Status != "live" {
			return nil, errors.New("connection status not live")
		}
		return conn, nil
	}
	return e.AddConnection(ctype)
}

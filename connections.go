// version: 0.0.1
// file to manage connection
package broker

import (
	errors "errors"
	log "log"
	time "time"

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

// create tls connection to borker
func (e *Broker) AddConnection(ctype string) (*Connection, error) {
	// create the dial
	connection, err := amqp.Dial(e.Endpoint)
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

				connection, err := amqp.Dial(e.Endpoint)
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

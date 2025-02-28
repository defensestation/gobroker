// version: 0.0.1
// file to manage connection
package gobroker

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
	conn := &Connection{
        Connection:  connection,
        Status:      "live",
        Type:        ctype,
        ChannelPool: map[int]*Channel{},
        pickCounter: 1,
    }
    e.connections[ctype] = conn

	// Start a goroutine to listen for connection close
    go func() {
        for {
            reason, ok := <-connection.NotifyClose(make(chan *amqp.Error))
            // If reason is nil, we still want some descriptive error
            if reason == nil {
                reason = &amqp.Error{
                    Code:   1337,
                    Reason: "Unknown: got reason=nil from NotifyClose",
                }
            }

            e.connections[ctype] = &Connection{
                Status:      reason.Reason,           // now "dead" or reason
                ChannelPool: map[int]*Channel{},      // empty
                pickCounter: 1,
            }

            // If !ok => the connection is closed by developer, just remove it
            if !ok {
                delete(e.connections, ctype)
                log.Printf("-> connection is closed by developer, ctype=%s", ctype)
                break
            }

            log.Printf("connection closed, reason: %v, ctype=%s", reason, ctype)

            // Attempt reconnect unless closed by developer
            for {
                time.Sleep(time.Duration(delay) * time.Second)

                newConn, err := amqp.Dial(e.Endpoint)
                if err == nil {
                    e.connections[ctype] = &Connection{
                        Connection:  newConn,
                        Status:      "live",
                        Type:        ctype,
                        ChannelPool: map[int]*Channel{},
                        pickCounter: 1,
                    }
                    log.Printf("connection re-established for ctype=%s", ctype)
                    break
                }

                log.Printf("reconnect failed, err: %v", err)
            }
        }
    }()

	return conn, nil
}

// GetConnection returns a live *Connection or tries to create one
func (e *Broker) GetConnection(ctype string) (*Connection, error) {
    raw, ok := e.connections[ctype]
    if !ok {
        // Not found => create
        return e.AddConnection(ctype)
    }

    conn, ok := raw.(*Connection)
    if !ok {
        return nil, errors.New("stored connection is not a *Connection")
    }
    if conn.Status != "live" {
        return nil, errors.New("connection status not live")
    }
    return conn, nil
}
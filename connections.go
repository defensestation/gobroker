// version: 0.0.1
// file to manage connection
package broker

import  (
	amqp "github.com/streadway/amqp"
	time "time"
	log  "log"
)

// connection types
var (
	PublishConnection 	= "publish"
	ConsumerConnection 	= "consume" 
)

type Connection struct {
	*amqp.Connection
	Type 	  	 string
	Status 	  	 string
	ChannelPool  map[string]*Channel
	lastPick 	 int
}

// create tls connection to borker
func (e *Exchange) AddConnection(ctype string) (*Connection, error) {
	// create the dial
	connection, err := amqp.Dial(e.broker.endpoint)
	if err != nil {
		return err
	}

	// set connection
	e.connections[ctype] = &Connection{
		connection,
		Status:   	"live",
		Type: 	  	ctype,
		ChannelPool: map[string]*Channel{},
		pickCounter: 1,
	}

	// start go routine that listen for connection close
 	go func() {
		 //Listen to NotifyClose
 		for {
			reason, ok := <-connection.NotifyClose(make(chan *amqp.Error))
			// reset channels and set channel status to dead
			e.connections[ctype] = &Connection{
						Status:   	 "dead",
						ChannelPool: map[string]*Channel{},
						pickCounter: 1,
					}
			// exit this goroutine if closed by developer
			if !ok {
				log.Printf("connection is closed")
				break
			}
			log.Printf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				connection, err := amqp.Dial(e.broker.endpoint)
				if err == nil {
					// set connection
					e.connections[ctype] = &Connection{
						connection,
						Status:   	 "live",
						Type: 	  	 ctype,
						ChannelPool: map[string]*Channel{},
						pickCounter: 1,
					}
					break
				}

				log.Printf("reconnect failed, err: %v", err)
			}
		}

	}()

    return e.connections[ctype], nil
}

func (e *Exchange) GetConnection(ctype string) (*Connection, error) {
	_, ok := e.connections[ctype]
	if ok {
		conn := e.connections[ctype]
		return conn, nil
	}
	return e.AddConnection(ctype)
}
// version: 0.0.1
// file to manage channel
package broker

import  (
	amqp "github.com/streadway/amqp"
	time "time"
	log  "log"
)

// channel limit per connection
var channelLimit = 50 // max can be 100

type Channel struct {
	*amqp.Channel
	Id int
}

// create channel for rabbitmq
func (c *Connection) AddChannel() (*Channel, error) {
	// check connection if live, if not retry to get the connection
	if c.Status != "live" {
		// retry to get the connection. maybe add multiple retries
		if err := c.setConnection(); err != nil {
			return nil, err
		}
	}

	//pool length 
	poolLength := len(c.ChannelPool)
	channelId  := poolLength + 1

	if poolLength > channelLimit {
		return nil, errors.New("channel limit reached")
	}

	// if connection is well create a channe
	ch, err := c.Channel()
   	if err != nil {
		return nil, err
	}

	// set the id of channel as index
	c.ChannelPool[channelId] = &Channel{ch, Id: channelId}

	// start go routine that listen for connection close
 	go func() {
		for {
			reason, ok := <-ch.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || ch.IsClosed() {
				log.Printf("channel closed with id:%v" channelId)
				channel.Close() // close again, ensure closed flag set when connection closed
				// delete channel from pool
				delete(c.ChannelPool, channelId)	
				break
			}
			log.Printf("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				ch, err := c.Channel()
				if err == nil {
					// update channel
					c.ChannelPool[channelId] = &Channel{ch, Id: channelId}
					log.Printf("channel recreate success")
					channel.Channel = ch
					break
				}

				log.Printf("channel recreate failed, err: %v", err)
			}
		}
	}()

	return ch, nil
}

// get channel can take id to get specific channel
func (c *Connection) GetChannel(id ...int) (*amqp.Channel, error) {
	// if id is provided return channel by that id
	if len(id) != 0 {
		_, ok := e.ChannelPool[id[0]]
		if ok {
			conn := e.ChannelPool[id[0]]
			return conn, id[0], nil
		}
		return nil,  errors.New("invalid id")
	}
	
	// get pool length
	poolLength := len(c.ChannelPool)

	// check if pool has any channels if not create and return
	if poolLength == 0 {
		return c.AddChannel()
	}

	//get channel
	ch := c.ChannelPool[c.pickCounter]
	// update pickcounter
	c.pickCounter =  (c.pickCounter % poolLength) + 1


	return ch,  nil

}
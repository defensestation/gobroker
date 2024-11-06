// version: 0.0.1
// file to manage channel
package broker

import (
	errors "errors"
	log "log"
	time "time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// channel limit per connection
var channelLimit = 50 // max can be 100

type Channel struct {
	*amqp.Channel
	Status string
	Id     int
}

// create channel for rabbitmq
func (c *Connection) AddChannel() (*Channel, error) {
	// check connection if live, if not retry to get the connection
	if c.Status != "live" {
		return nil, errors.New("connection not live")
	}

	// pool length
	poolLength := len(c.ChannelPool)
	channelId := poolLength + 1

	if poolLength > channelLimit {
		return nil, errors.New("channel limit reached")
	}

	// if connection is well create a channe
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}

	// set the id of channel as index
	channel := &Channel{ch, "live", channelId}
	c.ChannelPool[channelId] = channel

	// start go routine that listen for connection close
	go func() {
		for {
			reason, ok := <-channel.NotifyClose(make(chan *amqp.Error))

			// exit this goroutine if closed by developer
			if !ok {
				log.Printf("channel closed by developer with id:%v", channelId)
				ch.Close() // close again, ensure closed flag set when connection closed
				// delete channel from pool
				delete(c.ChannelPool, channelId)
				break
			}
			channel.Status = reason.Reason

			log.Printf("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				ch, err := c.Channel()
				if err == nil {
					// set the id of channel as index
					channel = &Channel{ch, "live", channelId}
					c.ChannelPool[channelId] = channel
					log.Printf("channel recreate success")
					break
				}

				log.Printf("channel recreate failed, err: %v", err)
			}
		}
	}()

	return c.ChannelPool[channelId], nil
}

// get channel can take id to get specific channel
func (c *Connection) GetChannel(id ...int) (*Channel, error) {
	// if id is provided return channel by that id
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

	// get pool length
	poolLength := len(c.ChannelPool)

	// check if pool has any channels if not create and return
	if poolLength == 0 {
		return c.AddChannel()
	}

	// get channel
	ch := c.ChannelPool[c.pickCounter]
	if ch == nil {
		return nil, errors.New("unable to find channel")
	}
	// check channel status
	if ch.Status != "live" {
		return ch, errors.New("channel status not live")
	}

	// update pickcounter
	c.pickCounter = (c.pickCounter % poolLength) + 1

	return ch, nil
}

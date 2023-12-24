// version: 0.0.1
// file to build exchange
package broker

import (
	errors "errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// exchane struct
type Exchange struct {
	broker      *Broker
	name        string
}

// exchange options
type ExchangeOptions struct {
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (eo *ExchangeOptions) defaultOpts() {
	// set defaults
	eo.Type = "topic"
	eo.Durable = true
}

// build exchange
func (b *Broker) BuildExchange(name string, opts ...*ExchangeOptions) (*Exchange, error) {
	// check exchange name
	if name == "" {
		return nil, errors.New("invalid name")
	}
	// create default exchange
	exchange := &Exchange{broker: b, name: name}

	// setup connections publisher connection
	publishConn, err := b.GetConnection(PublishConnection)
	if err != nil {
		return nil, err
	}

	// set default options
	options := &ExchangeOptions{}
	options.defaultOpts()
	// check if options provided
	if len(opts) != 0 {
		options = opts[0]
	}

	// get a connection channel
	ch, err := publishConn.GetChannel()
	if err != nil {
		return nil, err
	}
	// close this channel as we do not require this active channel until we publish
	defer ch.Close()

	// set exchange options
	err = ch.ExchangeDeclare(
		name,
		options.Type,
		options.Durable,
		options.AutoDelete,
		options.Internal,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		return nil, err
	}
	return exchange, nil
}
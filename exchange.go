// version: 0.0.1
// file to build exchange
package broker

// exchane struct
type Exchange struct {
    broker      *Broker
    name        string
    connections map[string]*Connection
}

// exchange options
type ExchangeOptions struct {
    Type            string  
    Durable         bool   
    AutoDelete      bool
    Internal        bool
    NoWait          bool
    Args            interface{}
}

func (eo *ExchangeOptions) defaultOpts() {
    eo = &ExchangeOptions{
        Type:       "topic",
        Durable     true,
    }
}

// build exchange
func (b *Broker) BuildExchange(name string, opts ...*ExchangeOptions) (*Exchange, error) {
    // create default exchange
    exchange := &Exchange{broker: b, name:name, connections: map[string]*Connection{}}

    // setup connections publisher connection
    publishConn, err := exchange.GetConnection(PublishConnection)
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
        return err
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
// version: 0.0.1
// file to run consumers

package broker

import  (
        amqp "github.com/streadway/amqp"
)


// only one channel is used per go cosumer
func (e *Exchange) RunConsumer(exchange, routeKey string, functions func([]byte), queueName string) (error) {
	// user consumer connection and add new channel for this routine
        ch, err := e.connections[ConsumerConnection].AddChannel()
        // check if any errors
        if err != nil {
                return err
        }

        // declare queue
        q, err  = ch.QueueDeclare(
                        queueName,              // name
                        true,                   // durable
                        false,                  // delete when unused
                        // usally when the qeueu exist only between service to broker name is not defined
                        // then it's a exclusive queue
                        (queueName == ""),      // exclusive
                        false,                  // no-wait
                        nil,                    // arguments
                )
        // check if any error      
        if err != nil {
                return err
        }

        // bind queue to echange
        err = ch.QueueBind(
                q.Name,       // queue name
                routeKey,     // routing key
                exchange,     // exchange
                false,        // no-wait
                nil,           // arguments
        )
        // check if any errors
        if err != nil {
                return  err
        }

        // build consumer
        msgs, err := ch.Consume(
                q.Name, // queue
                "",     // consumer
                true,   // auto ack
                false,  // exclusive
                false,  // no local
                false,  // no wait
                nil,    // args
        )

        // check if any errors
        if err != nil {
                return err
        }

        // start consumer connection and send every message to functoion
        go func() {
                // get the same channel in go routine
                ch, _ := e.connections[ConsumerConnection].GetChannel(ch.Id)
                for d := range msgs {
                        functions(d.Body)
                                                            
                }
                // close the channel with go routine ends
                defer ch.Close()
        }()

	return nil
}

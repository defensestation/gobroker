// version: 0.0.1
// file to run consumers

package gobroker

import (
	"log"
)

// only declare and bind
func (e *Exchange) QueueDeclareAndBind(exchange, routeKey, queueName string, ch *Channel) (string, error) {
	// declare queue
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		// usally when the qeueu exist only between service to broker name is not defined
		// then it's a exclusive queue
		(queueName == ""), // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	// check if any error
	if err != nil {
		return "", err
	}

	// bind queue to echange
	err = ch.QueueBind(
		q.Name,   // queue name
		routeKey, // routing key
		exchange, // exchange
		false,    // no-wait
		nil,      // arguments
	)
	// check if any errors
	if err != nil {
		return "", err
	}

	return q.Name, nil
}

// only one channel is used per go cosumer
func (e *Exchange) RunConsumer(exchange, routeKey string, functions func([]byte), queueName string) error {
	// get connection
	conn, err := e.broker.GetConnection(ConsumerConnection)
	if err != nil {
		return err
	}

	// user consumer connection and add new channel for this routine
	ch, err := conn.AddChannel()
	// check if any errors
	if err != nil {
		return err
	}

	qName, err := e.QueueDeclareAndBind(exchange, routeKey, queueName, ch)
	if err != nil {
		return err
	}

	// build consumer
	msgs, err := ch.Consume(
		qName, // queue
		"",    // consumer
		true,  // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil,   // args
	)
	// check if any errors
	if err != nil {
		return err
	}

	// start consumer connection and send every message to functoion
	go func() {
		rawConn := e.broker.connections[ConsumerConnection]
		conn, ok := rawConn.(*Connection)
		if !ok {
			log.Printf("failed to convert to *Connection") // log
			return                                         // exit goroutine
		}

		ch, err := conn.GetChannel(ch.Id)
		if err != nil {
			log.Printf("failed to get channel: %v", err) // log
			return
		}

		defer ch.Close() // ensure close even on error
		for d := range msgs {
			functions(d.Body)
		}
	}()

	return nil
}

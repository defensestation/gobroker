// version: 0.0.1
// file publish messages
package gobroker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishOptions struct {
	Mandatory bool
	Immediate bool
}

// expose method to publish messages to exchange
func (e *Exchange) Publish(ctx context.Context, routekey string, body interface{}, opts ...*PublishOptions) error {
	// marshal the golang interface to json
	jsonString, _ := json.Marshal(body)

	// get connection
	conn, err := e.broker.GetConnection(PublishConnection)
	if err != nil {
		return err
	}

	// pick connection and channel to publish
	ch, err := conn.GetChannel()
	if err != nil {
		return err
	}

	// do not close this channel. it will be used again for publishing messages
	// defer ch.Close()

	// validate routing key
	if !validRouteKey(routekey) {
		return errors.New("invalid routekey")
	}

	publisOps := &PublishOptions{}
	if len(opts) != 0 {
		publisOps = opts[0]
	}

	// publish message
	err = ch.PublishWithContext(
		ctx,
		e.name,              // exchange
		routekey,            // routing key
		publisOps.Mandatory, // mandatory
		publisOps.Immediate, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(jsonString),
		})
	// return err
	if err != nil {
		return err
	}
	return nil
}

// validate route key for format: servicename.event/log/*.*
// validate key must altest 3 parts
func validRouteKey(routekey string) bool {
	arr := strings.Split(routekey, ".")
	return len(arr) >= 3
}

// expose method to publish messages to exchange
func (b *Broker) PublishToExchange(ctx context.Context, exchangeName, routekey string, body interface{}, opts ...*PublishOptions) error {
	// marshal the golang interface to json
	jsonString, _ := json.Marshal(body)

	// get connection
	conn, err := b.GetConnection(PublishConnection)
	if err != nil {
		return err
	}

	// pick connection and channel to publish
	ch, err := conn.GetChannel()
	if err != nil {
		return err
	}

	// do not close this channel. it will be used again for publishing messages
	// defer ch.Close()

	// validate routing key
	if !validRouteKey(routekey) {
		return errors.New("invalid routekey")
	}

	publisOps := &PublishOptions{}
	if len(opts) != 0 {
		publisOps = opts[0]
	}
	// publish message
	err = ch.PublishWithContext(
		ctx,
		exchangeName,        // exchange
		routekey,            // routing key
		publisOps.Mandatory, // mandatory
		publisOps.Immediate, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(jsonString),
		})
	// return err
	if err != nil {
		return err
	}
	return nil
}

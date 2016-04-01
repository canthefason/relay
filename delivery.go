package relay

import (
	"bytes"

	"github.com/streadway/amqp"
)

// Delivery is a type that is used for having more control on consumed messages.
// Main purpose is having access to message properties
type Delivery struct {
	delivery       *amqp.Delivery
	serializer     Serializer
	enableMultiAck bool
}

// Decode will decode the delivery body with the serialization type set within the consumer
func (d *Delivery) Decode(out interface{}) error {
	buf := bytes.NewBuffer(d.delivery.Body)

	return d.serializer.RelayDecode(buf, out)
}

// Properties will extract the properties from amqp.Delivery, and
func (d *Delivery) Properties() *Properties {
	return &Properties{
		Headers:       map[string]interface{}(d.delivery.Headers),
		CorrelationId: d.delivery.CorrelationId,
		ReplyTo:       d.delivery.ReplyTo,
		MessageId:     d.delivery.MessageId,
		Type:          d.delivery.Type,
		UserId:        d.delivery.UserId,
		AppId:         d.delivery.AppId,
	}
}

// Ack will send an acknowledgement to the server for the delivery. It gets multiple acknowledge
// information from its consumer
func (d *Delivery) Ack() error {
	return d.delivery.Ack(d.enableMultiAck)
}

// Nack will send a negative acknowledgement to the server for the delivery. It gets multiple acknowledge
// information from its consumer
func (d *Delivery) Nack() error {
	// Requeing set true by default regarding to Consumer.Nack functionality, but
	// it will be better to have it configurable
	return d.delivery.Nack(d.enableMultiAck, true)
}

// Properties is a type that is used for setting and reading the amqp properties for each message
type Properties struct {
	Headers       map[string]interface{}
	CorrelationId string
	ReplyTo       string
	MessageId     string
	Type          string
	UserId        string
	AppId         string
}

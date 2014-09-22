package main

/**
 * Imports, yo
 */
import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"net/http"
)

/**
 * Log our error, vomit to screen, die
 */
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

/**
 * Main func.  Establish amqp connection, listen for messages, send to insights
 */
func main() {
	var AMQP_HOSTNAME string = "localhost-reactor.redventures.net"
	var AMQP_HOSTPORT string = "5672"
	var AMQP_USERNAME string = "guest"
	var AMQP_PASSWORD string = "guest"
	var AIO_QUEUE_NAME string = "aio-queue"

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", AMQP_USERNAME, AMQP_PASSWORD, AMQP_HOSTNAME, AMQP_HOSTPORT))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		AIO_QUEUE_NAME, // name
		false,          // durable
		false,          // delete when usused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,                // queue
		"aio-insights-worker", // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

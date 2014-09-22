package main

/**
 * Imports, yo
 */
import (
	"bytes"
	"flag"
	"fmt"
	"github.com/rakyll/globalconf"
	"github.com/streadway/amqp"
	"log"
	"net/http"
)

// Set up our allowed flags
var (
	amqp_hostname       = flag.String("amqp_hostname", "localhost", "The host address of the AMQP server you wish to listen to")
	amqp_hostport       = flag.String("amqp_hostport", "5672", "The host port of the AMQP server you wish to listen to")
	amqp_username       = flag.String("amqp_username", "guest", "The username you wish to auth to AMQP with")
	amqp_password       = flag.String("amqp_password", "guest", "The password you wish to auth to AMQP with")
	source_ampq_queue   = flag.String("aio_queue", "", "The source queue we are listening on")
	newrelic_account_id = flag.String("newrelic_account_id", "", "Your NewRelic account ID")
	newrelic_insert_key = flag.String("newrelic_insert_key", "", "Your NewRelic API key")
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
 * Send the given string payload to NewRelic
 */
func sendToInsights(payload []byte) {
	var url = fmt.Sprintf("https://insights-collector.newrelic.com/v1/accounts/%s/events", *newrelic_account_id)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	req.Header.Set("X-Insert-Key", *newrelic_insert_key)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	// Do you want to die on failure?
	failOnError(err, "Error sending to NewRelic")
	defer resp.Body.Close()
}

/**
 * Main func.  Establish amqp connection, listen for messages, send to insights
 */
func main() {
	// Spin up globalconf, overwriting flags where applicable
	conf, err := globalconf.New("myapp")
	failOnError(err, "Failed to load application configuration!")
	conf.ParseAll()
	flag.Parse()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", *amqp_username, *amqp_password, *amqp_hostname, *amqp_hostport))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		*source_ampq_queue, // name
		false,              // durable
		false,              // delete when usused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
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
			log.Printf("Msg: %s", d.Body)
			sendToInsights(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

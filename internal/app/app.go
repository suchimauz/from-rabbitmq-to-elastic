package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/suchimauz/golang-project-template/internal/config"
	"github.com/suchimauz/golang-project-template/pkg/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

type LogMessage struct {
	Index string `json:"index"`
	Log   string `json:"log"`
}

func Run() {
	// Initialize config
	cfg, err := config.NewConfig()
	if err != nil {
		logger.Errorf("[ENV] %s", err.Error())

		return
	}

	esCfg := elasticsearch.Config{
		Addresses: cfg.Elastic.Hosts,
	}
	esClient, err := elasticsearch.NewClient(esCfg)
	failOnError(err, "Failed to connect to Elasticsearch")

	rmqConn, err := amqp.Dial(cfg.RabbitMq.AmqpUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer rmqConn.Close()

	rmqCh, err := rmqConn.Channel()
	failOnError(err, "Failed to open a channel")
	defer rmqCh.Close()

	msgs, err := rmqCh.Consume(
		cfg.ConsumerQueueName, // queue
		"",                    // consumer
		true,                  // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			go func(d amqp.Delivery) {
				var logMsg LogMessage
				json.Unmarshal(d.Body, &logMsg)
				req := esapi.IndexRequest{
					Index: logMsg.Index,
					Body:  bytes.NewReader([]byte(logMsg.Log)),
				}

				res, err := req.Do(context.Background(), esClient)
				if err != nil {
					logger.Errorf("Error getting response: %s", err)
				}
				defer res.Body.Close()
			}(d)
		}
	}()

	logger.Info("RabbitMQ waiting messages...")

	// Graceful Shutdown

	// Make new channel of size = 1
	quit := make(chan os.Signal, 1)

	// Listen system 15 and 2 signals, when one of they called, send info to quit channel
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	// Read channel, this block of code lock this thread, until someone writes to the channel
	<-quit
}

func failOnError(err error, msg string) {
	if err != nil {
		logger.Panicf("%s: %s", msg, err)
	}
}

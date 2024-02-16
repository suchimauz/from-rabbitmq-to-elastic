package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
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
	failOnError(err, "Failed parse Environment")

	retryBackoff := backoff.NewExponentialBackOff()

	esCfg := elasticsearch.Config{
		Addresses: cfg.Elastic.Hosts,
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		//
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},

		// Retry up to 5 attempts
		//
		MaxRetries: 5,
	}
	esClient, err := elasticsearch.NewClient(esCfg)
	failOnError(err, "Failed to connect to Elasticsearch")

	esBulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        esClient,                                               // The Elasticsearch client
		NumWorkers:    cfg.Elastic.NumWorkers,                                 // The number of worker goroutines
		FlushBytes:    cfg.Elastic.FlushSize * 1000,                           // The flush threshold in bytes
		FlushInterval: time.Duration(cfg.Elastic.FlushInterval) * time.Second, // The periodic flush interval
	})
	failOnError(err, "Failed creating Elasticsearch Bulk Indexer")

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

	ticker := time.NewTicker(60 * time.Second)

	start := time.Now().UTC()

	go func() {
		for range ticker.C {
			esBulkIndexerStats := esBulkIndexer.Stats()
			log.Println(strings.Repeat("▔", 65))

			dur := time.Since(start)
			if esBulkIndexerStats.NumFailed > 0 {
				logger.Infof(
					"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
					humanize.Comma(int64(esBulkIndexerStats.NumFlushed)),
					humanize.Comma(int64(esBulkIndexerStats.NumFailed)),
					dur.Truncate(time.Millisecond),
					humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(esBulkIndexerStats.NumFlushed))),
				)
			} else {
				logger.Infof(
					"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
					humanize.Comma(int64(esBulkIndexerStats.NumFlushed)),
					dur.Truncate(time.Millisecond),
					humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(esBulkIndexerStats.NumFlushed))),
				)
			}
		}
	}()

	go func() {
		for d := range msgs {
			var logMsg LogMessage
			json.Unmarshal(d.Body, &logMsg)

			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
			//
			// Add an item to the BulkIndexer
			//
			err = esBulkIndexer.Add(
				context.Background(),
				esutil.BulkIndexerItem{
					Action: "index",
					Index:  logMsg.Index,
					// Body is an `io.Reader` with the payload
					Body: bytes.NewReader([]byte(logMsg.Log)),

					// OnFailure is called for each failed operation
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						if err != nil {
							logger.Errorf("ERROR: %s", err)
						} else {
							logger.Errorf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
						}
					},
				},
			)
			if err != nil {
				logger.Errorf("Unexpected error: %s", err)
			}
			// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
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

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	// Close the indexer
	//
	if err := esBulkIndexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
}

func failOnError(err error, msg string) {
	if err != nil {
		logger.Panicf("%s: %s", msg, err)
	}
}

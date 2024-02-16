package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

const (
	appName = "from-rabbitmq-to-elastic"
)

type (
	Config struct {
		timezoneEnv       string `envconfig:"app_timezone" default:"UTC"` // String timezone format
		Timezone          *time.Location
		ConsumerQueueName string `envconfig:"app_rabbitmq_consumer_queue_name"`
		Elastic           ElasticConfig
		RabbitMq          RabbitMqConfig
	}
	ElasticConfig struct {
		Hosts         []string `envconfig:"es_hosts" split_words:"true"`
		NumWorkers    int      `envconfig:"es_num_workers" default:"2"`
		FlushSize     int      `envconfig:"es_flush_size" default:"2048"`   // 2 mb (2048 KB)
		FlushInterval int      `envconfig:"es_flush_interval" default:"30"` // 30 seconds
	}
	RabbitMqConfig struct {
		Host     string `envconfig:"rabbitmq_host"`
		Port     string `envconfig:"rabbitmq_port"`
		User     string `envconfig:"rabbitmq_username"`
		Password string `envconfig:"rabbitmq_password"`
		Vhost    string `envconfig:"rabbitmq_vhost"`

		AmqpUri string
	}
)

func NewConfig() (*Config, error) {
	var cfg Config

	godotenv.Load()

	// Parse variables from environment or return err
	err := envconfig.Process(appName, &cfg)
	if err != nil {
		return nil, err
	}

	// Parse timezone from cfg.tz or return err
	cfg.Timezone, err = time.LoadLocation(cfg.timezoneEnv)
	if err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (cfg *Config) validate() error {
	if cfg.ConsumerQueueName == "" {
		return errors.New("APP_RABBITMQ_CONSUMER_QUEUE_NAME is required")
	}
	if len(cfg.Elastic.Hosts) == 0 {
		return errors.New("ES_HOSTS is required")
	}

	if err := cfg.RabbitMq.validateAndSetUri(); err != nil {
		return err
	}

	return nil
}

// Private func for set all FileStorageConfig fields required
func (rmqcfg *RabbitMqConfig) validateAndSetUri() error {
	if rmqcfg.Host == "" {
		return errors.New("RABBITMQ_HOST is required")
	}
	if rmqcfg.Port == "" {
		return errors.New("RABBITMQ_PORT is required")
	}
	if rmqcfg.User == "" {
		return errors.New("RABBITMQ_USERNAME is required")
	}
	if rmqcfg.Password == "" {
		return errors.New("RABBITMQ_PASSWORD is required")
	}

	rmqcfg.AmqpUri = fmt.Sprintf("amqp://%s:%s@%s:%s/%s", rmqcfg.User, rmqcfg.Password, rmqcfg.Host, rmqcfg.Port, rmqcfg.Vhost)

	return nil
}

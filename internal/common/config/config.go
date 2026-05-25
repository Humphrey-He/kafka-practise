package config

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	Kafka KafkaConfig `yaml:"kafka"`
}

type KafkaConfig struct {
	Brokers  []string       `yaml:"brokers"`
	ClientID string         `yaml:"client_id"`
	Version  string         `yaml:"version"`
	Producer ProducerConfig `yaml:"producer"`
	Consumer ConsumerConfig `yaml:"consumer"`
	TLS      TLSConfig      `yaml:"tls"`
	SASL     SASLConfig     `yaml:"sasl"`
}

type ProducerConfig struct {
	RequiredAcks    string `yaml:"required_acks"`
	RetryMax        int    `yaml:"retry_max"`
	ReturnSuccesses bool   `yaml:"return_successes"`
	Idempotent      bool   `yaml:"idempotent"`
}

type ConsumerConfig struct {
	GroupID       string `yaml:"group_id"`
	OffsetInitial string `yaml:"offset_initial"`
	AutoCommit    bool   `yaml:"auto_commit"`
}

type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	ServerName         string `yaml:"server_name"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

type SASLConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

func Load(path string) (*AppConfig, error) {
	cfg := defaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}
	applyEnv(cfg)
	return cfg, nil
}

func defaultConfig() *AppConfig {
	return &AppConfig{
		Kafka: KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "kafka-practise",
			Version:  "3.6.0",
			Producer: ProducerConfig{
				RequiredAcks:    "all",
				RetryMax:        3,
				ReturnSuccesses: true,
			},
			Consumer: ConsumerConfig{
				GroupID:       "kafka-practise-group",
				OffsetInitial: "oldest",
				AutoCommit:    false,
			},
		},
	}
}

func applyEnv(cfg *AppConfig) {
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = splitCSV(brokers)
	}
	if clientID := os.Getenv("KAFKA_CLIENT_ID"); clientID != "" {
		cfg.Kafka.ClientID = clientID
	}
	if groupID := os.Getenv("KAFKA_GROUP_ID"); groupID != "" {
		cfg.Kafka.Consumer.GroupID = groupID
	}
	if username := os.Getenv("KAFKA_SASL_USERNAME"); username != "" {
		cfg.Kafka.SASL.Username = username
	}
	if password := os.Getenv("KAFKA_SASL_PASSWORD"); password != "" {
		cfg.Kafka.SASL.Password = password
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

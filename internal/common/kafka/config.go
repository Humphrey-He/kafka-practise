package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	"github.com/IBM/sarama"
)

func NewSaramaConfig(cfg config.KafkaConfig) (*sarama.Config, error) {
	saramaCfg := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("parse kafka version %q: %w", cfg.Version, err)
	}
	saramaCfg.Version = version
	saramaCfg.ClientID = cfg.ClientID

	saramaCfg.Producer.RequiredAcks = requiredAcks(cfg.Producer.RequiredAcks)
	saramaCfg.Producer.Retry.Max = cfg.Producer.RetryMax
	saramaCfg.Producer.Retry.Backoff = 200 * time.Millisecond
	saramaCfg.Producer.Return.Successes = cfg.Producer.ReturnSuccesses
	saramaCfg.Producer.Return.Errors = true
	if cfg.Producer.Idempotent {
		saramaCfg.Producer.Idempotent = true
		saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
		if saramaCfg.Producer.Retry.Max == 0 {
			saramaCfg.Producer.Retry.Max = 3
		}
		saramaCfg.Net.MaxOpenRequests = 1
	}

	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = cfg.Consumer.AutoCommit
	saramaCfg.Consumer.Offsets.Initial = offsetInitial(cfg.Consumer.OffsetInitial)
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}

	if cfg.TLS.Enabled {
		tlsCfg, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		saramaCfg.Net.TLS.Enable = true
		saramaCfg.Net.TLS.Config = tlsCfg
	}

	if cfg.SASL.Enabled {
		saramaCfg.Net.SASL.Enable = true
		saramaCfg.Net.SASL.User = cfg.SASL.Username
		saramaCfg.Net.SASL.Password = cfg.SASL.Password
		saramaCfg.Net.SASL.Mechanism = saslMechanism(cfg.SASL.Mechanism)
	}

	return saramaCfg, nil
}

func requiredAcks(value string) sarama.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "none", "no_response":
		return sarama.NoResponse
	case "1", "local", "leader":
		return sarama.WaitForLocal
	case "all", "-1", "wait_for_all":
		return sarama.WaitForAll
	default:
		return sarama.WaitForAll
	}
}

func offsetInitial(value string) int64 {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "oldest", "earliest":
		return sarama.OffsetOldest
	case "newest", "latest":
		return sarama.OffsetNewest
	default:
		return sarama.OffsetOldest
	}
}

func saslMechanism(value string) sarama.SASLMechanism {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "SCRAM-SHA-256":
		return sarama.SASLTypeSCRAMSHA256
	case "SCRAM-SHA-512":
		return sarama.SASLTypeSCRAMSHA512
	case "OAUTHBEARER":
		return sarama.SASLTypeOAuth
	case "PLAIN", "":
		return sarama.SASLTypePlaintext
	default:
		return sarama.SASLMechanism(value)
	}
}

func buildTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.ServerName,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // demo config only; production docs explain the risk.
	}

	if cfg.CAFile != "" {
		ca, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("append ca certs from %s", cfg.CAFile)
		}
		tlsCfg.RootCAs = pool
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

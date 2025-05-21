package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var ErrUnsupportedSASLType = fmt.Errorf("unsupported SASL type")

type SASLType string

const (
	PLAIN       SASLType = "PLAIN"
	SCRAMSHA256 SASLType = "SCRAM-SHA-256"
	SCRAMSHA512 SASLType = "SCRAM-SHA-512"
)

type SecurityConfig struct {
	EnableTLS          bool
	SASL               bool
	SASLType           SASLType
	Username           string
	Password           string
	CACertificatePath  string
	ClientCertPath     string
	ClientKeyPath      string
	InsecureSkipVerify bool
}

func getSASLMechanism(config *SecurityConfig) (sasl.Mechanism, error) {
	switch config.SASLType {
	case PLAIN:
		return plain.Mechanism{Username: config.Username, Password: config.Password}, nil
	case SCRAMSHA256:
		return scram.Mechanism(scram.SHA256, config.Username, config.Password)
	case SCRAMSHA512:
		return scram.Mechanism(scram.SHA512, config.Username, config.Password)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedSASLType, config.SASLType)
	}
}

func createTLSConfig(config *SecurityConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	if config.CACertificatePath != "" {
		caCert, err := os.ReadFile(config.CACertificatePath)
		if err != nil {
			return nil, fmt.Errorf("error reading CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to add CA certificate to pool")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if config.ClientCertPath != "" && config.ClientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(config.ClientCertPath, config.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate and key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

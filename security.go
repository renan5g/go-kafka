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

type SASLType string

const (
	PLAIN       SASLType = "PLAIN"
	SCRAMSHA256 SASLType = "SCRAM-SHA-256"
	SCRAMSHA512 SASLType = "SCRAM-SHA-512"
)

// SecurityConfig contém configurações de segurança para conexão com Kafka
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
		return nil, fmt.Errorf("mecanismo SASL não suportado: %s", config.SASLType)
	}
}

func createTLSConfig(config *SecurityConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	// Carregar certificado CA se fornecido
	if config.CACertificatePath != "" {
		caCert, err := os.ReadFile(config.CACertificatePath)
		if err != nil {
			return nil, fmt.Errorf("erro ao ler certificado CA: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("falha ao adicionar certificado CA ao pool")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Carregar certificado e chave do cliente se fornecidos
	if config.ClientCertPath != "" && config.ClientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(config.ClientCertPath, config.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("erro ao carregar certificado e chave do cliente: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

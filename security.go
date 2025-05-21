package kafka

import (
	"crypto/tls"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var ErrUnsupportedSASLMechanismType = fmt.Errorf("unsupported SASL mechanism")

var DefaultSASLMechanismFactory = defaultSASLMechanismFactory

type SASLMechanismType int

type SASLMechanismFactory func(cfg *SASLConfig) (sasl.Mechanism, error)

const (
	PLAIN SASLMechanismType = iota
	SCRAM_SHA256
	SCRAM_SHA512
	AWS_MSK_IAM_V2
)

type SASLConfig struct {
	Type      SASLMechanismType
	Username  string
	Password  string
	AWSConfig *aws.Config
}

type SecurityConfig struct {
	EnableTLS  bool
	TLSConfig  *tls.Config
	EnableSASL bool
	SASL       *SASLConfig
}

func defaultSASLMechanismFactory(cfg *SASLConfig) (sasl.Mechanism, error) {
	switch cfg.Type {
	case PLAIN:
		return plain.Mechanism{Username: cfg.Username, Password: cfg.Password}, nil
	case SCRAM_SHA256:
		return scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
	case SCRAM_SHA512:
		return scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
	case AWS_MSK_IAM_V2:
		return aws_msk_iam_v2.NewMechanism(*cfg.AWSConfig), nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedSASLMechanismType, cfg.Type)
	}
}

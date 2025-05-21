package kafka

import (
	"github.com/segmentio/kafka-go"
)

type Message = kafka.Message

const (
	// Offsets
	FirstOffset = kafka.FirstOffset
	LastOffset  = kafka.LastOffset

	// Isolation levels
	ReadUncommitted = kafka.ReadUncommitted
	ReadCommitted   = kafka.ReadCommitted

	// Required acks
	RequireNone = kafka.RequireNone
	RequireOne  = kafka.RequireOne
	RequireAll  = kafka.RequireAll
)

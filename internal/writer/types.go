package writer

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type (
	// Writer is an interface which writes data in underlying dependency
	Writer interface {
		Write(ctx context.Context, data []byte) error
	}

	// StdWriter is a struct which implements writer interface and just logs the data to stdout
	StdWriter struct {
		log *zap.SugaredLogger
	}

	// NATSWriter is a struct which implements writer interface
	// and sends data into NATS Jetstream with provided subject
	NATSWriter struct {
		js         jetstream.JetStream
		log        *zap.SugaredLogger
		subject    string
		retryN     int
		retrySleep time.Duration
	}

	// NATSWriterOption is a optional function for configuring NATSWriter,
	// one can set retry count and sleep between retries
	NATSWriterOption func(*NATSWriter)
)

package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const (
	defaultWriterRetry = 5
	defaultWriterSleep = 5 * time.Second
)

// NewNATSWriter is a constructor for NATS Jetstream writer
func NewNATSWriter(js jetstream.JetStream, log *zap.SugaredLogger, subject string) *NATSWriter {
	log = log.With("subject", subject)

	return &NATSWriter{
		js:         js,
		log:        log,
		subject:    subject,
		retryN:     defaultWriterRetry,
		retrySleep: defaultWriterSleep,
	}
}

// Write publishes data into NATS Jetstream, if the error occures
// it tries `retryN` times with `retrySleep` in-between tries count
func (w *NATSWriter) Write(ctx context.Context, data []byte) error {
	var err error

	for range w.retryN {
		if _, err = w.js.Publish(ctx, w.subject, data); err != nil {
			w.log.Warnf("jetstream.Publish(): %v", err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.retrySleep):
				continue
			}
		}

		return nil
	}

	return fmt.Errorf("too many attempts to publish to JetStream. Last error: %w", err)
}

func WithRetryCount(count int) NATSWriterOption {
	return func(w *NATSWriter) {
		w.retryN = count
	}
}

func WithRetrySleep(duration time.Duration) NATSWriterOption {
	return func(w *NATSWriter) {
		w.retrySleep = duration
	}
}

// NewStdWriter is a constructor for standard writer
func NewStdWriter(log *zap.SugaredLogger) *StdWriter {
	return &StdWriter{
		log: log,
	}
}

func (w *StdWriter) Write(_ context.Context, _ []byte) error {
	return nil
}

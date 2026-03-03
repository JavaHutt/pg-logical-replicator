package snapshotter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/writer"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// NewStdProcessor is a constructor for standard processor
func NewStdProcessor(cfg config.Config, log *zap.SugaredLogger, w writer.Writer, batchSize int) *StdProcessor {
	return &StdProcessor{
		cfg:       cfg,
		log:       log,
		writer:    w,
		batchSize: batchSize,
	}
}

// Process gets the rows from DB, scans them and then sends to the writer.
// It returns last written `key` and `version` values
// If last batch is lower than batch size, meaning querying is dome, returns error io.EOF
func (p *StdProcessor) Process(ctx context.Context, rows pgx.Rows) (string, time.Time, error) {
	var (
		key     string
		version time.Time
		count   int
		err     error
	)

	for rows.Next() {
		payload := make(map[string]any)

		if err = rows.Scan(&key, &version, &payload); err != nil {
			return "", time.Time{}, fmt.Errorf("failed to scan snapshot row: %w", err)
		}

		data, err := prepareData(key, version, payload)
		if err != nil {
			return "", time.Time{}, err
		}

		if err = p.writer.Write(ctx, data); err != nil {
			return "", time.Time{}, err
		}

		count++
	}
	if rows.Err() != nil {
		return "", time.Time{}, fmt.Errorf("snapshot rows iteration failed: %w", rows.Err())
	}

	p.log.Infof("%d rows sent to writer", count)

	if count < p.batchSize {
		return key, version, io.EOF
	}

	return key, version, nil
}

func prepareData(key string, version time.Time, payload map[string]any) ([]byte, error) {
	m := message{
		Key:     key,
		Version: version,
		Payload: payload,
	}

	return json.Marshal(m)
}

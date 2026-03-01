package snapshotter

import (
	"context"
	"sync"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/writer"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type (
	// Snapshotter is a struct for a snapshotter object
	Snapshotter struct {
		svcName   string
		cfg       config.Config
		log       *zap.SugaredLogger
		conn      *pgx.Conn
		ctx       context.Context
		cancel    context.CancelFunc
		wg        sync.WaitGroup
		table     string
		processor Processor
		id        uuid.UUID
		startedAt time.Time
		batchSize int
	}

	// Option is a optional function for configuring replicator,
	// one can set service name and processor
	Option func(*Snapshotter)

	// SnapshotterDB is an object representing one row in snapshotters table
	SnapshotterDB struct {
		ID        uuid.UUID `db:"id"`
		StartedAt time.Time `db:"id"`
		TableName string    `db:"table_name"`
		Key       string    `db:"key"`
		Version   time.Time `db:"version"`
		IsDone    bool      `db:"is_done"`
	}

	message struct {
		Key     string
		Version time.Time
		Payload map[string]any
	}

	// Processor is an interface which process selected SQL rows
	Processor interface {
		Process(ctx context.Context, rows pgx.Rows) (string, time.Time, error)
	}

	// StdProcessor is a standart processor which implements Processor interface,
	// it is scanning SQL rows, serializing into message and then writes them into writer
	StdProcessor struct {
		cfg       config.Config
		log       *zap.SugaredLogger
		writer    writer.Writer
		batchSize int
	}
)

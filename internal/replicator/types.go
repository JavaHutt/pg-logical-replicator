//go:generate go run go.uber.org/mock/mockgen -source types.go -destination mock_processor.go -package replicator

package replicator

import (
	"context"
	"sync"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/writer"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type (
	// Processor is an interface which process received WAL data from logical replication
	Processor interface {
		Process(ctx context.Context, walData []byte, relations map[uint32]*pglogrepl.RelationMessage) error
	}

	// Replicator is a struct for a logical replication object
	Replicator struct {
		svcName        string
		cfg            config.Config
		ctx            context.Context
		cancel         context.CancelFunc
		wg             sync.WaitGroup
		log            *zap.SugaredLogger
		conn           *pgconn.PgConn
		processor      Processor
		slot           string
		publication    string
		table          string
		standByTimeout time.Duration
	}

	// Option is a optional function for configuring replicator,
	// one can set service name, processor and standby timeout
	Option func(*Replicator)

	message struct {
		Key     string
		Version time.Time
		Payload map[string]any
	}

	// StdProcessor is a standart processor which implements Processor interface,
	// it is parsing WAL message, retrieve values from INSERT and UPDATE messages,
	// and then writes them into writer
	StdProcessor struct {
		typeMap       *pgtype.Map
		cfg           config.Config
		log           *zap.SugaredLogger
		retrieveKeyFn retrieveKeyFn
		writer        writer.Writer
	}

	retrieveKeyFn func(any) (string, error)
)

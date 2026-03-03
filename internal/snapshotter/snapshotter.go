package snapshotter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/writer"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

const defaultSvcName = "snapshotter"

// New is a constructor
func New(cfg config.Config, log *zap.SugaredLogger, conn *pgx.Conn, table string, batchSize int, opts ...Option) *Snapshotter {
	s := Snapshotter{
		svcName:   defaultSvcName,
		cfg:       cfg,
		log:       log,
		conn:      conn,
		table:     table,
		processor: NewStdProcessor(cfg, log, writer.NewStdWriter(log), batchSize),
		batchSize: batchSize,
	}

	for _, opt := range opts {
		opt(&s)
	}

	return &s
}

// Run strats the snapshotter
func (s *Snapshotter) Run() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	defer s.wg.Done()

	sdb, err := s.getOrCreateSnapshotterFromDB(s.ctx, s.cfg.Snapshotter.ID, s.table)
	if err != nil {
		return err
	}
	if sdb.IsDone {
		s.log.Infof("snapshotter with id %s is already done, stopping snapshotter", sdb.ID)
		s.waitCtxDone()

		return nil
	}

	s.id = sdb.ID
	s.startedAt = sdb.StartedAt

	var (
		key     = sdb.Key
		version = sdb.Version
	)

	s.log.Infof("starting values: %s, %s", key, version)

	for {
		rows, err := s.querySnapshotData(s.ctx, key, version)
		if err != nil {
			return err
		}

		if key, version, err = s.processor.Process(s.ctx, rows); err != nil {
			if errors.Is(err, io.EOF) {
				if err = s.setSnapshotterDoneInDB(s.ctx, s.id, key, version); err != nil {
					return err
				}
				s.log.Infof("snapshotter with id %s is done", s.id)
				s.waitCtxDone()

				return nil
			}

			return fmt.Errorf("snapshot processor failed: %w", err)
		}
		rows.Close()

		if err = s.updateSnapshotterValuesInDB(s.ctx, s.id, key, version); err != nil {
			return err
		}

		s.log.Infof("batch inserted, last values: %s, %s", key, version)
	}
}

// Shutdown gracefully shuts down shapshotter
func (s *Snapshotter) Shutdown() {
	s.log.Debug("Shutdown started...")
	s.cancel()
	s.log.Debug("Context canceled")
	s.wg.Wait()
	s.log.Debug("Shutdown completed")
}

func (s *Snapshotter) querySnapshotData(ctx context.Context, key string, version time.Time) (pgx.Rows, error) {
	sql := s.prepareQuery()

	if key == "" {
		key = getPgTypeNullValueString(s.cfg.KeyColumn.PgType)
	}

	rows, err := s.conn.Query(ctx, sql, s.startedAt, version, key)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot data: %w", err)
	}

	return rows, nil
}

func (s *Snapshotter) getOrCreateSnapshotterFromDB(ctx context.Context, id uuid.UUID, table string) (*SnapshotterDB, error) {
	sdb, err := s.getSnapshotterFromDB(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if sdb, err = s.createSnapshotterInDB(ctx, id, table); err != nil {
				return nil, err
			}
			s.log.Infof("created new snapshotter with id %s", sdb.ID)

			return sdb, nil
		}

		return nil, err
	}

	s.log.Infof("using existing snapshotter with id %s", sdb.ID)

	return sdb, nil
}

func (s *Snapshotter) getSnapshotterFromDB(ctx context.Context, id uuid.UUID) (*SnapshotterDB, error) {
	var sdb SnapshotterDB
	row := s.conn.QueryRow(ctx, getSnapshotterQuery, id)

	if err := row.Scan(&sdb.ID, &sdb.StartedAt, &sdb.TableName, &sdb.Key, &sdb.Version, &sdb.IsDone); err != nil {
		return nil, fmt.Errorf("failed to scan snapshotter row: %w", err)
	}

	return &sdb, nil
}

func (s *Snapshotter) createSnapshotterInDB(ctx context.Context, id uuid.UUID, table string) (*SnapshotterDB, error) {
	now := time.Now().UTC()

	_, err := s.conn.Exec(ctx, createSnapshotterQuery, id, table, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert snapshotter row: %w", err)
	}

	return &SnapshotterDB{
		ID:        id,
		StartedAt: now,
	}, nil
}

func (s *Snapshotter) updateSnapshotterValuesInDB(ctx context.Context, id uuid.UUID, key string, version time.Time) error {
	if _, err := s.conn.Exec(ctx, updateSnapshotterQuery, key, version, id); err != nil {
		return fmt.Errorf("failed to update snapshotter row: %w", err)
	}

	return nil
}

func (s *Snapshotter) setSnapshotterDoneInDB(ctx context.Context, id uuid.UUID, key string, version time.Time) error {
	if _, err := s.conn.Exec(ctx, updateSnapshotterDoneQuery, key, version, true, id); err != nil {
		return fmt.Errorf("failed to mark snapshotter done: %w", err)
	}

	return nil
}

func (s *Snapshotter) prepareQuery() string {
	sortingTuple := strings.Join(s.cfg.Snapshotter.SortingTuple, ", ")
	return fmt.Sprintf(selectDataQuery, s.cfg.KeyColumn.Name, s.cfg.Version, s.table, sortingTuple, s.batchSize)
}

func (s *Snapshotter) waitCtxDone() {
	<-s.ctx.Done()
}

func WithSvcName(name string) Option {
	return func(s *Snapshotter) {
		if name != "" {
			s.svcName = name
		}
	}
}

func WithProcessor(processor Processor) Option {
	return func(s *Snapshotter) {
		s.processor = processor
	}
}

func getPgTypeNullValueString(pgType string) string {
	switch pgType {
	case "uuid":
		return uuid.Nil.String()
	case "varchar":
		return ""
	case "bigserial":
		return "0"
	default:
		return ""
	}
}

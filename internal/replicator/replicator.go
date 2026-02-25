package replicator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/writer"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.uber.org/zap"
)

const (
	defaultSvcName = "replicator"
	defaultTimeout = 10 * time.Second
	outputPlugin   = "pgoutput"
)

// New is a constructor
func New(
	cfg config.Config,
	log *zap.SugaredLogger,
	conn *pgconn.PgConn,
	opts ...Option,
) *Replicator {
	table := cfg.Table
	log = log.With("module", fmt.Sprintf("%s_replicator", table))

	r := Replicator{
		svcName:        defaultSvcName,
		cfg:            cfg,
		log:            log,
		conn:           conn,
		processor:      MustNewStdProcessor(cfg, log, writer.NewStdWriter(log)),
		slot:           fmt.Sprintf("%s_slot", table),
		publication:    fmt.Sprintf("%s_publication", table),
		table:          table,
		standByTimeout: defaultTimeout,
	}

	for _, opt := range opts {
		opt(&r)
	}

	return &r
}

// Run strats the replicator
func (r *Replicator) Run() error {
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	defer r.wg.Done()

	err := r.createPublication(r.ctx, r.publication, r.table)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	lsn, err := r.startReplication(r.ctx, r.slot, r.publication)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	if err = r.receiveMessages(r.ctx, lsn); err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down replicator
func (r *Replicator) Shutdown() {
	r.log.Debug("Shutdown started...")
	r.cancel()
	r.log.Debug("Context canceled")
	r.wg.Wait()
	r.log.Debug("Shutdown completed")
}

func (r *Replicator) receiveMessages(ctx context.Context, clientXLogPos pglogrepl.LSN) error {
	var (
		nextStandbyMessageDeadline = time.Now().Add(r.standByTimeout)
		relations                  = make(map[uint32]*pglogrepl.RelationMessage)
		err                        error
	)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("failed to send standby status update: %w", err)
			}
			r.log.Infof("sent Standby status message at %s", clientXLogPos)
			nextStandbyMessageDeadline = time.Now().Add(r.standByTimeout)
		}

		ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			r.log.Warnf("received unexpected message type: %T", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			if err = processKeepAliveMessage(msg, &clientXLogPos, &nextStandbyMessageDeadline); err != nil {
				return err
			}

		case pglogrepl.XLogDataByteID:
			if err = r.processXLogData(r.ctx, msg, &clientXLogPos, relations); err != nil {
				return err
			}
		}
	}
}

func (r *Replicator) startReplication(ctx context.Context, slot, publication string) (pglogrepl.LSN, error) {
	startLSN, err := r.createReplicationSlot(ctx, slot, false)
	if err != nil {
		return 0, fmt.Errorf("failed to create replication slot: %w", err)
	}
	r.log.Infof("start LSN %s", startLSN)

	pluginArguments := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publication),
	}

	err = pglogrepl.StartReplication(ctx, r.conn, slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return 0, fmt.Errorf("failed to start replication: %w", err)
	}
	r.log.Infof("logical replication started on slot %s", slot)

	return startLSN, nil
}

func (r *Replicator) createPublication(ctx context.Context, publication string, table string) error {
	sql := fmt.Sprintf("SELECT pubname FROM pg_catalog.pg_publication_tables WHERE pubname = '%s';", publication)
	results, err := r.conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	res := results[0]
	if len(res.Rows) != 0 {
		r.log.Infof("using existing publication %s", publication)

		return nil
	}

	result := r.conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %q FOR TABLE %q;", publication, table))
	if _, err := result.ReadAll(); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}
	r.log.Infof("created publication %s for table: %s", publication, table)

	return nil
}

func (r *Replicator) createReplicationSlot(ctx context.Context, slot string, temporary bool) (pglogrepl.LSN, error) {
	_, err := pglogrepl.CreateReplicationSlot(ctx, r.conn, slot, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.DuplicateObject {
			r.log.Infof("using existing slot %s", slot)

			return r.getReplicationSlotLSN(ctx, slot)
		}
		return 0, fmt.Errorf("failed to create replication slot: %w", err)
	}

	r.log.Infof("created replication slot: %s", slot)

	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return 0, fmt.Errorf("failed to identify system: %w", err)
	}

	return sysident.XLogPos, nil
}

func (r *Replicator) getReplicationSlotLSN(ctx context.Context, slot string) (pglogrepl.LSN, error) {
	sql := fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s';", slot)
	results, err := r.conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return 0, fmt.Errorf("failed to get replication slot LSN: %w", err)
	}

	res := results[0]
	return pglogrepl.ParseLSN(string(res.Rows[0][0]))
}

func (r *Replicator) processXLogData(
	ctx context.Context,
	msg *pgproto3.CopyData,
	clientXLogPos *pglogrepl.LSN,
	relations map[uint32]*pglogrepl.RelationMessage,
) error {
	xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
	if err != nil {
		return fmt.Errorf("failed to parse xlog data: %w", err)
	}

	if err = r.processor.Process(ctx, xld.WALData, relations); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	if xld.WALStart > *clientXLogPos {
		*clientXLogPos = xld.WALStart
	}

	return nil
}

func WithSvcName(name string) Option {
	return func(r *Replicator) {
		if name != "" {
			r.svcName = name
		}
	}
}

func WithStandByTimeout(timeout time.Duration) Option {
	return func(r *Replicator) {
		if timeout != 0 {
			r.standByTimeout = timeout
		}
	}
}

func WithProcessor(processor Processor) Option {
	return func(r *Replicator) {
		r.processor = processor
	}
}

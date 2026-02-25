package replicator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/writer"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

// MustNewStdProcessor is a constructor for standard processor.
// If incorrect key column pg_type is passed within config, it panics.
func MustNewStdProcessor(cfg config.Config, log *zap.SugaredLogger, w writer.Writer) *StdProcessor {
	retrieveKeyFn, err := getRetrieveKeyFn(cfg.KeyColumn.PgType)
	if err != nil {
		log.Fatal(err)
	}

	return &StdProcessor{
		typeMap:       pgtype.NewMap(),
		cfg:           cfg,
		log:           log,
		retrieveKeyFn: retrieveKeyFn,
		writer:        w,
	}
}

// Processor parses WAL message, retrieve values from INSERT and UPDATE messages,
// and then writes them into writer
func (p *StdProcessor) Process(
	ctx context.Context,
	walData []byte,
	relations map[uint32]*pglogrepl.RelationMessage,
) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse logical message: %w", err)
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.BeginMessage:
	case *pglogrepl.CommitMessage:
	case *pglogrepl.InsertMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}

		values, err := getValuesFromColumns(p.typeMap, rel, logicalMsg.Tuple.Columns)
		if err != nil {
			return err
		}

		if err = p.write(ctx, values); err != nil {
			return fmt.Errorf("failed to write insert message: %w", err)
		}
	case *pglogrepl.UpdateMessage:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}

		values, err := getValuesFromColumns(p.typeMap, rel, logicalMsg.NewTuple.Columns)
		if err != nil {
			return err
		}

		if err = p.write(ctx, values); err != nil {
			return fmt.Errorf("failed to write update message: %w", err)
		}
	case *pglogrepl.DeleteMessage:
	case *pglogrepl.TruncateMessage:
	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:
	case *pglogrepl.LogicalDecodingMessage:
	default:
		p.log.Warnf("unknown message type in pgoutput stream: %T", logicalMsg)
	}

	return nil
}

func processKeepAliveMessage(msg *pgproto3.CopyData, clientXLogPos *pglogrepl.LSN, nextStandbyMessageDeadline *time.Time) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
	if err != nil {
		return fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}

	if pkm.ServerWALEnd > *clientXLogPos {
		*clientXLogPos = pkm.ServerWALEnd
	}

	if pkm.ReplyRequested {
		*nextStandbyMessageDeadline = time.Time{}
	}

	return nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}

	return string(data), nil
}

func getValuesFromColumns(typeMap *pgtype.Map, rel *pglogrepl.RelationMessage, columns []*pglogrepl.TupleDataColumn) (map[string]any, error) {
	values := make(map[string]any, len(columns))

	for idx, col := range columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("error decoding column data: %w", err)
			}
			values[colName] = val
		}
	}

	return values, nil
}

func (p *StdProcessor) write(ctx context.Context, values map[string]any) error {
	var (
		msg message
		key string
		err error
	)

	if key, err = p.retrieveKey(values); err != nil {
		return fmt.Errorf("failed to retrieve key: %w", err)
	}
	msg.Key = key

	if msg.Version, err = retrieveVersion(values, p.cfg.Version); err != nil {
		return fmt.Errorf("failed to marshal version: %w", err)
	}

	msg.Payload = values

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if err = p.writer.Write(ctx, payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	p.log.Infof("sent message with key %s", key)

	return nil
}

func (p *StdProcessor) retrieveKey(values map[string]any) (string, error) {
	return p.retrieveKeyFn(values[p.cfg.KeyColumn.Name])
}

func getRetrieveKeyFn(pgType string) (retrieveKeyFn, error) {
	switch pgType {
	case "uuid":
		return retrieveUUIDKey, nil

	case "bigserial":
		return retrieveIntKey, nil

	case "varchar":
		return retrieveStrKey, nil

	default:
		return nil, errors.New("unknown pg_type")
	}
}

func retrieveUUIDKey(value any) (string, error) {
	b, ok := value.([16]byte)
	if !ok {
		return "", errors.New("missing or invalid uuid key field")
	}
	u := uuid.UUID(b)

	return u.String(), nil
}

func retrieveIntKey(value any) (string, error) {
	i, ok := value.(int64)
	if !ok {
		return "", errors.New("missing or invalid int key field")
	}

	return strconv.Itoa(int(i)), nil
}

func retrieveStrKey(value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", errors.New("missing or invalid string key field")
	}

	return s, nil
}

func retrieveVersion(values map[string]any, col string) (time.Time, error) {
	updatedAt, ok := values[col].(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("missing or invalid %s version field", col)
	}

	return updatedAt, nil
}

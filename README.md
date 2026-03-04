# pg-logical-replicator

A high-performance PostgreSQL logical replication tool that streams table snapshots and changes to NATS JetStream.

## Overview

`pg-logical-replicator` efficiently replicates PostgreSQL data using two complementary approaches:

- **Snapshotter**: Performs a full table snapshot on demand, reading rows in batches
- **Replicator**: Continuously streams incremental changes using PostgreSQL's logical decoding

Both components publish data to [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) for reliable, ordered message delivery.

## Getting Started

### Prerequisites

- Go 1.26+
- PostgreSQL 10+ (with logical replication support)
- NATS Server 2.2+ (with JetStream enabled)

### Environment Variables

Create a `.env` file:

```env
# NATS Configuration
NATS_ADDRESS=127.0.0.1:4222
NATS_USER=replicator
NATS_PASSWORD=nats_password

# PostgreSQL Configuration
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=mydb
DATABASE_USER=postgres
DATABASE_PASS=postgres
DATABASE_SCHEMA=public

# Application Configuration
CONFIGS_PATH=.env/config.json
BATCH_SIZE=1000
STANDBY_TIMEOUT=10s
LOGGER_LEVEL=info
```

### Replication Configuration

Create a JSON config file (`.env/config.json`):

```json
[
  {
    "table": "transactions",
    "subject": "pg.transactions",
    "key_column": {
      "name": "id",
      "pg_type": "bigserial"
    },
    "version": "updated_at",
    "snapshotter": {
      "enabled": true,
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "sorting_tuple": ["updated_at", "id"]
    },
    "replicator": {
      "enabled": true
    }
  }
]
```

**Configuration Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `table` | string | PostgreSQL table name |
| `subject` | string | NATS JetStream subject |
| `key_column.name` | string | Primary key column |
| `key_column.pg_type` | string | Type: `uuid`, `varchar`, `bigserial` |
| `version` | string | Column for tracking changes |
| `snapshotter.enabled` | boolean | Enable initial snapshot |
| `snapshotter.id` | UUID | Unique snapshot state ID |
| `snapshotter.sorting_tuple` | array | Query sort columns |
| `replicator.enabled` | boolean | Enable continuous replication |

### Running

```bash
make run
```

## How It Works

### Snapshotter

1. Checks metadata table for existing state
2. Fetches rows in batches using a cursor query
3. Publishes rows to NATS subject
4. Updates progress in metadata table
5. Marks as done when complete

### Replicator

1. Creates a publication for the table
2. Creates or resumes a replication slot
3. Streams WAL changes using pgoutput plugin
4. Publishes INSERT/UPDATE/DELETE events to NATS
5. Sends periodic standby status updates to PostgreSQL

## Custom Output Sources

By default, data is published to [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream), but you can implement your own output destination by implementing the `Writer` interface:

```go
type Writer interface {
  Write(ctx context.Context, records []Record) error
}
```

Examples: Kafka, Redis Streams, PostgreSQL, S3, Webhook, etc. See `internal/writer/writer.go` for the current NATS implementation.

## PostgreSQL Setup

Enable logical replication:

```sql
SHOW wal_level;  -- Should be 'logical' or 'replica'
ALTER USER postgres REPLICATION;
```

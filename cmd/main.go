package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"pg-logical-replicator/internal/config"
	"pg-logical-replicator/internal/replicator"
	"pg-logical-replicator/internal/snapshotter"
	"pg-logical-replicator/internal/writer"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/oklog/run"
	"go.uber.org/zap"
)

type envConfig struct {
	// Main
	ConfigsPath    string        `env:"CONFIGS_PATH,required"`
	BatchSize      int           `env:"BATCH_SIZE" envDefault:"1000"`
	StandByTimeout time.Duration `env:"STANDBY_TIMEOUT" envDefault:"10s"`

	// Health / Metrics
	HealthPort     int    `env:"HEALTH_PORT" envDefault:"7777"`
	MetricsPort    int    `env:"METRICS_PORT" envDefault:"7777"`
	HealthEndpoint string `env:"HEALTH_ENDPOINT" envDefault:"/health"`

	// NATS
	NATSAddress  string `env:"NATS_ADDRESS,required"`
	NATSUser     string `env:"NATS_USER" envDefault:""`
	NATSPassword string `env:"NATS_PASSWORD" envDefault:""`

	// DB
	DBHost     string `env:"DATABASE_HOST,required"`
	DBPort     int    `env:"DATABASE_PORT,required"`
	DBName     string `env:"DATABASE_NAME,required"`
	DBUsername string `env:"DATABASE_USER,required"`
	DBPassword string `env:"DATABASE_PASS,required"`
	DBSchema   string `env:"DATABASE_SCHEMA" envDefault:"public"`
	DBSSLMode  string `env:"DATABASE_SSL_MODE" envDefault:"disable"`

	// Logging
	LogLevel string `env:"LOGGER_LEVEL" envDefault:"info"`
}

func loadEnvConfig(envPath string) (envConfig, error) {
	if envPath != "" {
		if err := godotenv.Load(envPath); err != nil && !os.IsNotExist(err) {
			return envConfig{}, fmt.Errorf("load .env: %w", err)
		}
	} else {
		_ = godotenv.Load(".env/.env.example", ".env/.env.local")
	}

	var cfg envConfig
	if err := env.Parse(&cfg); err != nil {
		return envConfig{}, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}

func newLogger(cfg envConfig) (*zap.SugaredLogger, error) {
	level, err := zap.ParseAtomicLevel(cfg.LogLevel)
	if err != nil {
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	zapCfg := zap.Config{
		Level:         level,
		Development:   false,
		Encoding:      "json",
		EncoderConfig: zap.NewProductionEncoderConfig(),
		OutputPaths:   []string{"stdout"},
		InitialFields: map[string]any{"service": "pg-logical-replicator"},
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build logger: %w", err)
	}

	return logger.Sugar(), nil
}

func main() {
	cfg, err := loadEnvConfig("")
	if err != nil {
		log.Fatal(err)
	}

	logger, err := newLogger(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	logger.Info("logger initialized")

	nc, err := natsConnect(cfg.NATSAddress, cfg.NATSUser, cfg.NATSPassword)
	if err != nil {
		logger.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Fatal(err)
	}

	configData, err := os.ReadFile(cfg.ConfigsPath)
	if err != nil {
		logger.Fatalf("read configs: %v", err)
	}

	configs, err := config.ParseConfigs(configData)
	if err != nil {
		logger.Fatal(err)
	}

	ctx := context.Background()
	var g run.Group

	// snapshotters
	for _, snapCfg := range configs {
		if !snapCfg.Snapshotter.Enabled {
			continue
		}

		pg, err := openDB(ctx, cfg.DBUsername, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBSSLMode, cfg.DBSchema)
		if err != nil {
			logger.Fatal(err)
		}
		defer pg.Close(ctx)

		table := snapCfg.Table
		snapLogger := logger.With("table", table)

		processor := snapshotter.NewStdProcessor(snapCfg, snapLogger, writer.NewNATSWriter(js, snapLogger, snapCfg.Subject), cfg.BatchSize)
		snap := snapshotter.New(snapCfg, snapLogger, pg, table, cfg.BatchSize, snapshotter.WithProcessor(processor))

		g.Add(
			func() error {
				snapLogger.Infof("snapshotter started")
				err := snap.Run()
				snapLogger.Infof("snapshotter finished: %v", err)
				return err
			},
			func(err error) {
				snap.Shutdown()
				snapLogger.Infof("snapshotter shut down: %v", err)
			},
		)
	}

	// replicators
	for _, replCfg := range configs {
		if !replCfg.Replicator.Enabled {
			continue
		}

		replPG, err := openReplDB(ctx, cfg.DBUsername, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBSSLMode, cfg.DBSchema)
		if err != nil {
			logger.Fatal(err)
		}
		defer replPG.Close(ctx)

		table := replCfg.Table
		replLogger := logger.With("table", table)

		processor := replicator.MustNewStdProcessor(replCfg, replLogger, writer.NewNATSWriter(js, replLogger, replCfg.Subject))
		repl := replicator.New(
			replCfg,
			replLogger,
			replPG,
			replicator.WithProcessor(processor),
			replicator.WithStandByTimeout(cfg.StandByTimeout),
		)

		g.Add(
			func() error {
				replLogger.Infof("replicator started")
				err := repl.Run()
				replLogger.Infof("replicator finished: %v", err)
				return err
			},
			func(err error) {
				repl.Shutdown()
				replLogger.Infof("replicator shut down: %v", err)
			},
		)
	}

	if err := g.Run(); err != nil {
		logger.Fatalf("run group: %v", err)
	}
}

func openReplDB(ctx context.Context, user, password, host string, port int, dbName, sslMode, searchPath string) (*pgconn.PgConn, error) {
	dsn := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s search_path=%s replication=database",
		user, password, host, port, dbName, sslMode, searchPath)

	config, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse replication pg config: %w", err)
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to replication pg: %w", err)
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping replication pg: %w", err)
	}

	return conn, nil
}

func openDB(ctx context.Context, user, password, host string, port int, dbName, sslMode, searchPath string) (*pgx.Conn, error) {
	dsn := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s search_path=%s",
		user, password, host, port, dbName, sslMode, searchPath)

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse replication pg config: %w", err)
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to replication pg: %w", err)
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping replication pg: %w", err)
	}

	return conn, nil
}

func natsConnect(addr, user, password string) (*nats.Conn, error) {
	opts := []nats.Option{}
	if user != "" && password != "" {
		opts = append(opts, nats.UserInfo(user, password))
	}

	conn, err := nats.Connect(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return conn, nil
}

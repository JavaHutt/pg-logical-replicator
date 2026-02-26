package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"pg-logical-replicator/internal/config"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	_ = configs
	_ = js
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

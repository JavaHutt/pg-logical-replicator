package config

import (
	"encoding/json"

	"github.com/google/uuid"
)

type (
	snapshotterConfig struct {
		Enabled      bool      `json:"enabled"`
		ID           uuid.UUID `json:"id"`
		SortingTuple []string  `json:"sorting_tuple"`
	}

	replicatorConfig struct {
		Enabled bool `json:"enabled"`
	}

	KeyColumn struct {
		Name   string `json:"name"`
		PgType string `json:"pg_type"`
	}

	Config struct {
		Table       string            `json:"table"`
		Subject     string            `json:"subject"`
		KeyColumn   KeyColumn         `json:"key_column"`
		Version     string            `json:"version"`
		Snapshotter snapshotterConfig `json:"snapshotter"`
		Replicator  replicatorConfig  `json:"replicator"`
	}
)

func ParseConfigs(b []byte) ([]Config, error) {
	var configsWrapper struct {
		Configs []Config `json:"configs"`
	}

	if err := json.Unmarshal(b, &configsWrapper); err != nil {
		return nil, err
	}

	return configsWrapper.Configs, nil
}

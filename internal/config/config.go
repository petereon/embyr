package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the complete runtime configuration for firstyr.
type Config struct {
	Server       ServerConfig      `mapstructure:"server"`
	Auth         AuthConfig        `mapstructure:"auth"`
	Backend      BackendConfig     `mapstructure:"backend"`
	Transactions TransactionConfig `mapstructure:"transactions"`
	Log          LogConfig         `mapstructure:"log"`
}

// ServerConfig controls the gRPC and REST listener ports and optional TLS.
type ServerConfig struct {
	GRPCPort int       `mapstructure:"grpc_port"`
	RESTPort int       `mapstructure:"rest_port"`
	TLS      TLSConfig `mapstructure:"tls"`
}

// TLSConfig holds paths to the server certificate and private key.
type TLSConfig struct {
	Cert string `mapstructure:"cert"`
	Key  string `mapstructure:"key"`
}

// AuthConfig selects the authentication mode and its associated parameters.
type AuthConfig struct {
	Mode            string `mapstructure:"mode"`
	Key             string `mapstructure:"key"`
	GoogleProjectID string `mapstructure:"google_project_id"`
	MTLSCACert      string `mapstructure:"mtls_ca"`
}

// BackendConfig selects the storage backend and its driver-specific options.
type BackendConfig struct {
	Type     string         `mapstructure:"type"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	SQLite   SQLiteConfig   `mapstructure:"sqlite"`
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	DSN      string `mapstructure:"dsn"`
	MaxConns int    `mapstructure:"max_conns"`
}

// SQLiteConfig holds SQLite connection settings.
type SQLiteConfig struct {
	Path string `mapstructure:"path"`
}

// TransactionConfig controls transaction TTL and expired-transaction sweep frequency.
type TransactionConfig struct {
	TTL           time.Duration `mapstructure:"ttl"`
	SweepInterval time.Duration `mapstructure:"sweep_interval"`
}

// LogConfig selects the log level and output format.
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load reads a YAML config file (path may be empty for defaults only).
// Environment variables prefixed with FIRSTYR_ override any config file value.
// Key mapping: FIRSTYR_AUTH_KEY -> auth.key, FIRSTYR_BACKEND_TYPE -> backend.type
func Load(path string) (*Config, error) {
	v := viper.New()

	v.SetDefault("server.grpc_port", 8080)
	v.SetDefault("server.rest_port", 8081)
	v.SetDefault("auth.mode", "none")
	v.SetDefault("backend.type", "sqlite")
	v.SetDefault("backend.sqlite.path", "firstyr.db")
	v.SetDefault("backend.postgres.max_conns", 25)
	v.SetDefault("transactions.ttl", 60*time.Second)
	v.SetDefault("transactions.sweep_interval", 30*time.Second)
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	v.SetEnvPrefix("FIRSTYR")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicitly bind env vars so Unmarshal picks them up even when only
	// defaults are set. AutomaticEnv covers Get calls but not Unmarshal.
	envBindings := []string{
		"server.grpc_port",
		"server.rest_port",
		"auth.mode",
		"auth.key",
		"auth.google_project_id",
		"auth.mtls_ca",
		"backend.type",
		"backend.sqlite.path",
		"backend.postgres.dsn",
		"backend.postgres.max_conns",
		"transactions.ttl",
		"transactions.sweep_interval",
		"log.level",
		"log.format",
	}
	for _, key := range envBindings {
		if err := v.BindEnv(key); err != nil {
			return nil, fmt.Errorf("config: bind env %s: %w", key, err)
		}
	}

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

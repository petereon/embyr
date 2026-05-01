package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the complete runtime configuration for embyr.
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
	// AllowedOrigins lists CORS origins. Empty slice = allow all (dev default).
	AllowedOrigins []string `mapstructure:"allowed_origins"`
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
// Environment variables prefixed with EMBYR_ override any config file value.
// Key mapping: EMBYR_AUTH_KEY -> auth.key, EMBYR_BACKEND_TYPE -> backend.type
func Load(path string) (*Config, error) {
	v := viper.New()

	v.SetDefault("server.grpc_port", 8080)
	v.SetDefault("server.rest_port", 8081)
	v.SetDefault("auth.mode", "none")
	v.SetDefault("backend.type", "sqlite")
	v.SetDefault("backend.sqlite.path", "embyr.db")
	v.SetDefault("backend.postgres.max_conns", 25)
	v.SetDefault("transactions.ttl", 60*time.Second)
	v.SetDefault("transactions.sweep_interval", 30*time.Second)
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	v.SetEnvPrefix("EMBYR")
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks semantic constraints that viper/mapstructure cannot enforce.
func (c *Config) Validate() error {
	if c.Server.GRPCPort < 1 || c.Server.GRPCPort > 65535 {
		return fmt.Errorf("config: server.grpc_port %d out of range [1,65535]", c.Server.GRPCPort)
	}
	if c.Server.RESTPort < 1 || c.Server.RESTPort > 65535 {
		return fmt.Errorf("config: server.rest_port %d out of range [1,65535]", c.Server.RESTPort)
	}
	if c.Server.GRPCPort == c.Server.RESTPort {
		return fmt.Errorf("config: server.grpc_port and server.rest_port must differ (both %d)", c.Server.GRPCPort)
	}
	if c.Transactions.TTL <= 0 {
		return fmt.Errorf("config: transactions.ttl must be positive, got %s", c.Transactions.TTL)
	}
	if c.Transactions.SweepInterval <= 0 {
		return fmt.Errorf("config: transactions.sweep_interval must be positive, got %s", c.Transactions.SweepInterval)
	}
	return nil
}

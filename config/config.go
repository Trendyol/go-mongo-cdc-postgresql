package config

import (
	"time"

	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
)

// PostgreSQL holds the sink-specific configuration for the PostgreSQL connector.
type PostgreSQL struct {
	// DSN is the PostgreSQL connection string.
	// e.g. "host=localhost user=postgres password=postgres dbname=mydb port=5432 sslmode=disable"
	DSN string `yaml:"dsn"`

	// Host/Port/User/Password/Database/SSLMode are used to build a DSN when DSN is empty.
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslMode"`

	// BatchSizeLimit is the maximum number of actions per batch before a flush is triggered.
	BatchSizeLimit int `yaml:"batchSizeLimit"`

	// BatchTickerDuration flushes the batch automatically on a fixed interval for long-waiting messages.
	BatchTickerDuration time.Duration `yaml:"batchTickerDuration"`

	// BatchCommitTickerDuration configures checkpoint offset save time. When nil,
	// offsets are committed immediately after each batch flush.
	BatchCommitTickerDuration *time.Duration `yaml:"batchCommitTickerDuration"`

	// ConcurrentRequest is the number of concurrent batch executions against PostgreSQL.
	ConcurrentRequest int `yaml:"concurrentRequest"`

	// MaxOpenConns / MaxIdleConns / ConnMaxLifetime / ConnMaxIdleTime control the underlying pool.
	MaxOpenConns    int           `yaml:"maxOpenConns"`
	MaxIdleConns    int           `yaml:"maxIdleConns"`
	ConnMaxLifetime time.Duration `yaml:"connMaxLifetime"`
	ConnMaxIdleTime time.Duration `yaml:"connMaxIdleTime"`

	// StatementTimeout is applied per batch execution (0 means no timeout).
	StatementTimeout time.Duration `yaml:"statementTimeout"`

	// LogLevel controls GORM's own logger: "silent", "error", "warn", "info".
	LogLevel string `yaml:"logLevel"`
}

type Config struct {
	PostgreSQL PostgreSQL       `yaml:"postgresql"`
	CDC        cdcConfig.Config `yaml:",inline" mapstructure:",squash"`
}

func (c *Config) ApplyDefaults() {
	if c.PostgreSQL.BatchTickerDuration == 0 {
		c.PostgreSQL.BatchTickerDuration = 10 * time.Second
	}

	if c.PostgreSQL.BatchSizeLimit == 0 {
		c.PostgreSQL.BatchSizeLimit = 1000
	}

	if c.PostgreSQL.ConcurrentRequest == 0 {
		c.PostgreSQL.ConcurrentRequest = 1
	}

	if c.PostgreSQL.MaxOpenConns == 0 {
		c.PostgreSQL.MaxOpenConns = 10
	}

	if c.PostgreSQL.MaxIdleConns == 0 {
		c.PostgreSQL.MaxIdleConns = 5
	}

	if c.PostgreSQL.ConnMaxLifetime == 0 {
		c.PostgreSQL.ConnMaxLifetime = 30 * time.Minute
	}

	if c.PostgreSQL.SSLMode == "" {
		c.PostgreSQL.SSLMode = "disable"
	}

	if c.PostgreSQL.Port == 0 {
		c.PostgreSQL.Port = 5432
	}

	if c.PostgreSQL.LogLevel == "" {
		c.PostgreSQL.LogLevel = "warn"
	}
}

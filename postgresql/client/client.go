package client

import (
	"fmt"

	"github.com/Trendyol/go-mongo-cdc-postgresql/config"
	"github.com/Trendyol/go-mongo-cdc/logger"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// NewClient creates a GORM DB handle configured from the connector config.
func NewClient(cfg *config.Config) (*gorm.DB, error) {
	dsn := buildDSN(cfg.PostgreSQL)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                 &LoggerAdapter{Level: resolveLogLevel(cfg.PostgreSQL.LogLevel)},
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open postgresql connection: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to access underlying sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(cfg.PostgreSQL.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.PostgreSQL.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.PostgreSQL.ConnMaxLifetime)
	if cfg.PostgreSQL.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.PostgreSQL.ConnMaxIdleTime)
	}

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgresql: %w", err)
	}

	logger.Log.Info("connected to postgresql")
	return db, nil
}

func buildDSN(pg config.PostgreSQL) string {
	if pg.DSN != "" {
		return pg.DSN
	}
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pg.Host, pg.Port, pg.User, pg.Password, pg.Database, pg.SSLMode,
	)
}

func resolveLogLevel(level string) gormlogger.LogLevel {
	switch level {
	case "silent":
		return gormlogger.Silent
	case "error":
		return gormlogger.Error
	case "info":
		return gormlogger.Info
	default:
		return gormlogger.Warn
	}
}

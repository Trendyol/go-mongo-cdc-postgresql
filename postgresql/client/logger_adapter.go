package client

import (
	"context"
	"time"

	"github.com/Trendyol/go-mongo-cdc/logger"
	gormlogger "gorm.io/gorm/logger"
)

// LoggerAdapter bridges GORM's logger interface to go-mongo-cdc's logger.
type LoggerAdapter struct {
	Level gormlogger.LogLevel
}

func (l *LoggerAdapter) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l
	newLogger.Level = level
	return &newLogger
}

func (l *LoggerAdapter) Info(_ context.Context, msg string, data ...any) {
	if l.Level >= gormlogger.Info {
		logger.Log.Info(msg, data...)
	}
}

func (l *LoggerAdapter) Warn(_ context.Context, msg string, data ...any) {
	if l.Level >= gormlogger.Warn {
		logger.Log.Warn(msg, data...)
	}
}

func (l *LoggerAdapter) Error(_ context.Context, msg string, data ...any) {
	if l.Level >= gormlogger.Error {
		logger.Log.Error(msg, data...)
	}
}

func (l *LoggerAdapter) Trace(_ context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.Level <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	switch {
	case err != nil && l.Level >= gormlogger.Error:
		logger.Log.Error("gorm trace error: %v | %s | rows: %d | elapsed: %s", err, sql, rows, elapsed)
	case l.Level >= gormlogger.Info:
		logger.Log.Debug("gorm trace: %s | rows: %d | elapsed: %s", sql, rows, elapsed)
	}
}

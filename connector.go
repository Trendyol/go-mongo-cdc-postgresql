package cdcpostgresql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	cdc "github.com/Trendyol/go-mongo-cdc"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"github.com/Trendyol/go-mongo-cdc/stream"

	"github.com/Trendyol/go-mongo-cdc-postgresql/config"
	"github.com/Trendyol/go-mongo-cdc-postgresql/helper"
	"github.com/Trendyol/go-mongo-cdc-postgresql/metric"
	"github.com/Trendyol/go-mongo-cdc-postgresql/mongodb"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql/bulk"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql/client"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	cdc                 cdc.Connector
	mapper              Mapper
	config              *config.Config
	bulk                *bulk.Bulk
	sinkResponseHandler postgresql.SinkResponseHandler
	closing             int32
}

func (c *connector) Start(ctx context.Context) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)
	defer stop()

	go func() {
		c.bulk.StartBulk()
	}()

	cdcCtx, cdcCancel := context.WithCancel(context.Background())
	defer cdcCancel()

	go func() {
		select {
		case <-ctx.Done():
			logger.Log.Debug("Shutdown signal received: interrupt, initiating graceful shutdown...")
			atomic.StoreInt32(&c.closing, 1)

			logger.Log.Debug("Stopped accepting new events, waiting for in-flight events to complete...")
			c.bulk.Close()

			logger.Log.Debug("Bulk closed, committing final checkpoints before closing CDC")
			cdcCancel()
		case <-cdcCtx.Done():
		}
	}()

	c.cdc.Start(cdcCtx)
}

func (c *connector) Close() {
	atomic.StoreInt32(&c.closing, 1)
	c.bulk.Close()
	c.cdc.Close()
}

func (c *connector) listener(ctx *stream.ListenerContext) error {
	if atomic.LoadInt32(&c.closing) == 1 {
		logger.Log.Debug("Rejecting new event during shutdown - documentId: %v, partitionId: %d", ctx.Message.DocumentID, ctx.PartitionID)
		return nil
	}

	select {
	case <-ctx.Context.Done():
		return ctx.Context.Err()
	default:
	}

	var e mongodb.Event
	switch ctx.Message.OperationType {
	case message.OperationInsert, message.OperationUpdate, message.OperationReplace:
		if ctx.Message.FullDocument == nil {
			ctx.Ack()
			return nil
		}

		doc := ctx.Message.FullDocument
		delete(doc, "_id")

		docBytes, err := jsoniter.Marshal(doc)
		if err != nil {
			logger.Log.Error("Failed to marshal document to JSON, skipping event: %v", err)
			ctx.Ack()
			return nil
		}

		e = mongodb.NewMutateEvent(
			ctx.Message.DocumentID,
			docBytes,
			doc,
			ctx.Message.Collection,
			ctx.Message.EventTime,
			ctx.PartitionID,
		)
	case message.OperationDelete:
		e = mongodb.NewDeleteEvent(
			ctx.Message.DocumentID,
			ctx.Message.Collection,
			ctx.Message.EventTime,
			ctx.PartitionID,
		)
	default:
		ctx.Ack()
		return nil
	}

	actions := c.mapper(e)
	if len(actions) == 0 {
		ctx.Ack()
		return nil
	}

	batchSizeLimit := c.config.PostgreSQL.BatchSizeLimit
	if len(actions) > batchSizeLimit {
		chunks := helper.ChunkSliceWithSize[postgresql.Action](actions, batchSizeLimit)
		lastChunkIndex := len(chunks) - 1
		for idx, chunk := range chunks {
			c.bulk.AddActions(ctx, e.EventTime, chunk, idx == lastChunkIndex, ctx.PartitionID, ctx.IsBootstrap)
		}
	} else {
		c.bulk.AddActions(ctx, e.EventTime, actions, true, ctx.PartitionID, ctx.IsBootstrap)
	}

	return nil
}

func documentIDToString(id any) string {
	switch v := id.(type) {
	case primitive.ObjectID:
		return v.Hex()
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case string:
		return v
	default:
		logger.Log.Warn("Unexpected document ID type: %T, value: %v", id, id)
		return fmt.Sprintf("%v", id)
	}
}

func newConnectorConfigFromPath(path string) (*config.Config, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	envPattern := regexp.MustCompile(`\${([^}]+)}`)
	matches := envPattern.FindAllStringSubmatch(string(file), -1)
	for _, match := range matches {
		envVar := match[1]
		if value, exists := os.LookupEnv(envVar); exists {
			updatedFile := strings.ReplaceAll(string(file), "${"+envVar+"}", value)
			file = []byte(updatedFile)
		}
	}
	var c config.Config
	if err = yaml.Unmarshal(file, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func newConfig(cf any) (*config.Config, error) {
	switch v := cf.(type) {
	case *config.Config:
		return v, nil
	case config.Config:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func newConnector(cf any, mapper Mapper, sinkResponseHandler postgresql.SinkResponseHandler, zapLogger *zap.Logger) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	if mapper == nil {
		mapper = DefaultMapper
	}

	conn := &connector{
		mapper:              mapper,
		config:              cfg,
		sinkResponseHandler: sinkResponseHandler,
	}

	cdcCfg := cdcConfig.Config{
		MongoDB:                 cfg.CDC.MongoDB,
		Metric:                  cfg.CDC.Metric,
		Logger:                  cfg.CDC.Logger,
		Checkpoint:              cfg.CDC.Checkpoint,
		Partition:               cfg.CDC.Partition,
		GracefulShutdownTimeout: cfg.CDC.GracefulShutdownTimeout,
	}
	cdcCfg.Checkpoint.Type = "manual"

	cdcConnector, err := cdc.NewConnector(cdcCfg, conn.listener)
	if err != nil {
		return nil, err
	}
	conn.cdc = cdcConnector

	// Re-apply custom logger after cdc.NewConnector, which initializes its own
	// default logger and overwrites logger.Log.
	if zapLogger != nil {
		logger.Log = &logger.Loggers{Zap: zapLogger}
	}

	printConfiguration(cfg.PostgreSQL)

	db, err := client.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	checkpointCommit := func() {
		cdcConnector.Commit()
	}
	checkpointCommitBootstrap := func(partitionID int) {
		cdcConnector.CommitBootstrap(partitionID)
	}

	conn.bulk, err = bulk.NewBulk(
		cfg,
		checkpointCommit,
		checkpointCommitBootstrap,
		db,
		sinkResponseHandler,
		metric.NewMetricsRecorder(),
	)
	if err != nil {
		return nil, err
	}

	eventHandler := &CdcEventHandler{bulk: conn.bulk}
	cdcConnector.SetEventHandler(eventHandler)

	return conn, nil
}

type ConnectorBuilder struct {
	mapper              Mapper
	config              any
	sinkResponseHandler postgresql.SinkResponseHandler
	zapLogger           *zap.Logger
}

func NewConnectorBuilder(config any) *ConnectorBuilder {
	return &ConnectorBuilder{
		config: config,
	}
}

func (c *ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper, c.sinkResponseHandler, c.zapLogger)
}

func (c *ConnectorBuilder) SetMapper(mapper Mapper) *ConnectorBuilder {
	c.mapper = mapper
	return c
}

func (c *ConnectorBuilder) SetLogger(zapLogger *zap.Logger) *ConnectorBuilder {
	c.zapLogger = zapLogger
	return c
}

func (c *ConnectorBuilder) SetSinkResponseHandler(sinkResponseHandler postgresql.SinkResponseHandler) *ConnectorBuilder {
	c.sinkResponseHandler = sinkResponseHandler
	return c
}

func printConfiguration(cfg config.PostgreSQL) {
	cfg.Password = "*****"
	cfg.DSN = "*****"
	configJSON, _ := jsoniter.Marshal(cfg)

	dst := &bytes.Buffer{}
	if err := json.Compact(dst, configJSON); err != nil {
		logger.Log.Error("error while printing postgresql configuration, err: %v", err)
		panic(err)
	}

	logger.Log.Info("using postgresql config: %v", dst.String())
}

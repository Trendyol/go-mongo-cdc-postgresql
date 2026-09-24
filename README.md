# Go Mongo CDC PostgreSQL [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-mongo-cdc-postgresql.svg)](https://pkg.go.dev/github.com/Trendyol/go-mongo-cdc-postgresql) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-mongo-cdc-postgresql)](https://goreportcard.com/report/github.com/Trendyol/go-mongo-cdc-postgresql)

MongoDB Change Data Capture (CDC) connector for PostgreSQL. This library provides
real-time data synchronization from MongoDB to PostgreSQL using MongoDB Change
Streams.

It builds on top of [`go-mongo-cdc`](https://github.com/Trendyol/go-mongo-cdc) for
MongoDB CDC and adds a configurable PostgreSQL sink layer powered by
[GORM](https://gorm.io).

## Features

- **Near real-time synchronization** from MongoDB to PostgreSQL using MongoDB Change Streams.
- **GORM-based sink**: works with your own GORM models; the table and primary key are inferred from the struct.
- **Custom mapping** via pluggable mapper functions that can emit one or many PostgreSQL actions per change event.
- **Batch processing controls** such as maximum batch size and batch ticker durations for efficient bulk writes.
- **Upsert & delete**: mutate events become `INSERT ... ON CONFLICT DO UPDATE`, delete events become `DELETE` by primary key.
- **Multi-table batches**: a single batch can target multiple tables; actions are grouped per table and executed together.
- **Concurrent execution** of batch groups against PostgreSQL.
- **Easily manageable configurations** via YAML files or Go structs.
- **Prometheus metrics** exposed on the same endpoint as the underlying CDC layer.
- **Raw SQL escape hatch**: emit arbitrary `Query + Args` statements from a mapper and run them through the same batching and checkpoint pipeline.

## Installation

```
go get github.com/Trendyol/go-mongo-cdc-postgresql
```

## Quick Start

### Basic Usage (Default Mapper)

The default mapper writes each document into a generic `documents` table
(`id TEXT PRIMARY KEY`, `data JSONB`). Create the table first:

```sql
CREATE TABLE documents (
    id   TEXT PRIMARY KEY,
    data JSONB NOT NULL
);
```

```go
package main

import (
    "context"

    cdcpostgresql "github.com/Trendyol/go-mongo-cdc-postgresql"
)

func main() {
    connector, err := cdcpostgresql.NewConnectorBuilder("config.yml").Build()
    if err != nil {
        panic(err)
    }
    defer connector.Close()

    connector.Start(context.Background())
}
```

### Custom Mapper (Your Own GORM Model)

Define a GORM model and map events to it. You control the target table and
primary key; GORM handles the rest.

```go
type User struct {
    ID    string `gorm:"column:id;primaryKey" json:"_id"`
    Name  string `gorm:"column:name"          json:"name"`
    Email string `gorm:"column:email"         json:"email"`
}

func mapper(event mongodb.Event) []postgresql.Action {
    if event.IsMutated {
        var u User
        _ = jsoniter.Unmarshal(event.Value, &u)
        u.ID = event.DocumentID.(string)
        return []postgresql.Action{postgresql.NewUpsertAction(&u)}
    }
    return []postgresql.Action{
        postgresql.NewDeleteAction(&User{ID: event.DocumentID.(string)}),
    }
}

connector, err := cdcpostgresql.NewConnectorBuilder(cfg).
    SetMapper(mapper).
    Build()
```

### Raw SQL Actions

A mapper can emit arbitrary SQL for cases GORM models can't express (multi-table
fan-out, custom casts, triggers, ...). Raw actions go through the same batching
and checkpoint pipeline; tag them with `TableName` so they keep arrival order
with that table's other actions:

```go
func mapper(event mongodb.Event) []postgresql.Action {
    id := event.DocumentID.(string)
    actions := []postgresql.Action{
        postgresql.NewUpsertAction(&User{ID: id, Name: name}),
    }
    // extra statement executed after the upsert, in order:
    actions = append(actions, postgresql.NewRawActionWithTable(
        "user_audit_log",
        "INSERT INTO user_audit_log (user_id, op) VALUES (?, ?)", id, "sync",
    ))
    return actions
}
```

## Configuration

`go-mongo-cdc-postgresql` uses two main configuration blocks:

- **PostgreSQL (`postgresql`)**: connection, pooling and batching settings for the sink.
- **CDC**: MongoDB connection, partitioning, checkpointing, metrics and logging
  (identical to `go-mongo-cdc`, provided inline).

Configuration can be supplied either as a YAML file path (`"config.yml"`), a Go
struct (`config.Config`), or a pointer to a Go struct.

### Example `config.yml`

```yaml
postgresql:
  host: "localhost"
  port: 5432
  user: "postgres"
  password: "postgres"
  database: "mydb"
  sslMode: "disable"
  batchSizeLimit: 1000
  batchTickerDuration: 10s
  concurrentRequest: 1
  maxOpenConns: 10
  maxIdleConns: 5
  logLevel: "warn"

mongodb:
  connection:
    uri: "localhost:27017"
    database: "exampleDB"
    collection: "exampleCollection"
  connectionPool:
    maxPoolSize: 100
    minPoolSize: 5

metric:
  port: 8080

checkpoint:
  tokenSaveInterval: 10s
  changeStreamSaveCount: 500
  bootstrapQueryBatchSize: 5000
  bootstrapSaveInterval: 10s

partition:
  totalPartition: 1
  heartbeatInterval: 10s
  consumerGroup: "postgresql"

logger:
  logLevel: "info"

gracefulShutdownTimeout: 5s
```

### PostgreSQL Configuration

### Sink Settings (`postgresql`)

| Variable                              | Type          | Required | Default | Description                                                                 |
|---------------------------------------|---------------|----------|---------|-----------------------------------------------------------------------------|
| `postgresql.dsn`                      | string        | no       | -       | Full DSN (e.g. `host=localhost user=pg password=pg dbname=cdcdb port=5432 sslmode=disable`). When set, `host`/`port`/`user`/... are ignored. |
| `postgresql.host`                     | string        | yes*     | -       | PostgreSQL host (used when `dsn` is empty).                                  |
| `postgresql.port`                     | int           | no       | 5432    | PostgreSQL port.                                                            |
| `postgresql.user`                     | string        | yes*     | -       | Username.                                                                   |
| `postgresql.password`                 | string        | no       | -       | Password.                                                                   |
| `postgresql.database`                 | string        | yes*     | -       | Database name.                                                              |
| `postgresql.sslMode`                  | string        | no       | disable | SSL mode (`disable`, `require`, `verify-ca`, `verify-full`).                |
| `postgresql.batchSizeLimit`           | int           | no       | 1000    | Maximum number of actions per batch; if exceeded, a flush is triggered.     |
| `postgresql.batchTickerDuration`      | time.Duration | no       | 10s     | Batch is flushed automatically at this interval for long-waiting messages.  |
| `postgresql.batchCommitTickerDuration`| time.Duration | no       | -       | Checkpoint offset save interval; when unset, offsets are committed after each batch flush. |
| `postgresql.concurrentRequest`        | int           | no       | 1       | Number of batch groups executed concurrently against PostgreSQL.            |
| `postgresql.maxOpenConns`             | int           | no       | 10      | Maximum open connections in the pool.                                       |
| `postgresql.maxIdleConns`             | int           | no       | 5       | Maximum idle connections in the pool.                                       |
| `postgresql.connMaxLifetime`          | time.Duration | no       | 30m     | Maximum lifetime of a connection.                                           |
| `postgresql.connMaxIdleTime`          | time.Duration | no       | -       | Maximum idle time of a connection.                                          |
| `postgresql.statementTimeout`         | time.Duration | no       | 0       | Per-batch execution timeout (`0` = no timeout).                             |
| `postgresql.logLevel`                 | string        | no       | warn    | GORM logger level: `silent`, `error`, `warn`, `info`.                       |

\* Required only when `dsn` is empty.

### MongoDB / CDC Configuration

CDC configuration is identical to `go-mongo-cdc`. Common fields:

#### Connection & Pool (`mongodb.connection`, `mongodb.connectionPool`, `mongodb.timeouts`)

| Variable                                   | Type   | Required | Default | Description                              |
|--------------------------------------------|--------|----------|---------|------------------------------------------|
| `mongodb.connection.uri`                   | string | yes      | -       | MongoDB connection URI (host:port). The library prepends `mongodb://`. |
| `mongodb.connection.database`              | string | yes      | -       | MongoDB database.                        |
| `mongodb.connection.collection`            | string | yes      | -       | MongoDB collection.                      |
| `mongodb.connection.username`              | string | no       | -       | MongoDB username for authentication.     |
| `mongodb.connection.password`              | string | no       | -       | MongoDB password for authentication.     |
| `mongodb.connectionPool.maxPoolSize`       | uint64 | no       | 100     | Maximum number of connections in the pool. |
| `mongodb.connectionPool.minPoolSize`       | uint64 | no       | 5       | Minimum number of connections to maintain. |
| `mongodb.connectionPool.maxIdleTimeMS`     | int64  | no       | 300000  | Maximum idle time of a connection (ms).  |
| `mongodb.timeouts.connectTimeoutMS`        | int64  | no       | 30000   | Connection timeout (ms).                 |
| `mongodb.timeouts.serverSelectionTimeoutMS`| int64  | no       | 60000   | Server selection timeout (ms).           |
| `mongodb.timeouts.socketTimeoutMS`         | int64  | no       | 120000  | Socket timeout (ms).                     |

#### Checkpoint (`checkpoint`)

> **Note:** The checkpoint `type` is forced to `manual` internally. Checkpoints
> are committed only after a batch is successfully written to PostgreSQL, ensuring
> at-least-once delivery.

| Variable                            | Type          | Required | Default | Description                                    |
|-------------------------------------|---------------|----------|---------|------------------------------------------------|
| `checkpoint.tokenSaveInterval`      | time.Duration | no       | 10s     | Change stream resume token save interval.      |
| `checkpoint.saveTimeout`            | time.Duration | no       | 10s     | Token save timeout.                            |
| `checkpoint.changeStreamSaveCount`  | int           | no       | 500     | Change stream batch size.                      |
| `checkpoint.bootstrapSaveCount`     | int           | no       | 2500    | Documents to process before bootstrap save.    |
| `checkpoint.bootstrapSaveInterval`  | time.Duration | no       | 10s     | Bootstrap checkpoint save interval.            |
| `checkpoint.bootstrapQueryBatchSize`| int           | no       | 2500    | Bootstrap batch size.                          |
| `checkpoint.idleHeartbeatInterval`  | time.Duration | no       | 3m      | Idle heartbeat interval.                       |
| `checkpoint.maxIdleTime`            | time.Duration | no       | 15m     | Max idle time before partition release.        |

#### Partition (`partition`)

| Variable                          | Type          | Required | Default                | Description                                         |
|-----------------------------------|---------------|----------|------------------------|-----------------------------------------------------|
| `partition.consumerGroup`         | string        | yes      | -                      | Unique consumer group name; suffix on internal collections. |
| `partition.totalPartition`        | int           | no       | 15                     | Total partitions. Stored on first init; cannot change afterwards. |
| `partition.heartbeatInterval`     | time.Duration | no       | 10s                    | Worker heartbeat interval.                          |
| `partition.workerTimeout`         | time.Duration | no       | 90s                    | Worker timeout.                                     |
| `partition.rebalanceCheckInterval`| time.Duration | no       | 10s                    | Partition rebalance check interval.                 |
| `partition.workersCollection`     | string        | no       | `workers`              | Workers collection name.                            |
| `partition.partitionsCollection`  | string        | no       | `partition_assignments`| Partition assignments collection name.              |

#### Metric, Logger & Shutdown

| Variable                 | Type          | Required | Default | Description                                  |
|--------------------------|---------------|----------|---------|----------------------------------------------|
| `metric.port`            | int           | no       | 8080    | Prometheus metrics port.                     |
| `logger.logLevel`        | string        | no       | info    | **CDC log level**: `error`, `warn`, `info`, `debug`. |
| `gracefulShutdownTimeout`| time.Duration | no       | 10s     | Max time to wait for in-flight events on shutdown. |

> **Note on `LogLevel`:** there are **two** independent log levels:
> - `logger.logLevel` → controls the CDC connector logs (`error`/`warn`/`info`/`debug`).
> - `postgresql.logLevel` → controls GORM's SQL logging (`silent`/`error`/`warn`/`info`).

For the full `go-mongo-cdc` configuration reference, see
[go-mongo-cdc#configuration](https://github.com/Trendyol/go-mongo-cdc#configuration).

## SinkResponseHandler

Implement `postgresql.SinkResponseHandler` to receive per-action callbacks:

```go
type handler struct{}

func (handler) OnInit(ctx *postgresql.SinkResponseHandlerInitContext) {}
func (handler) OnSuccess(ctx *postgresql.SinkResponseHandlerContext)  {}
func (handler) OnError(ctx *postgresql.SinkResponseHandlerContext)    {}

connector, err := cdcpostgresql.NewConnectorBuilder(cfg).
    SetSinkResponseHandler(handler{}).
    Build()
```

When no handler is set, a failed batch causes a panic (fail-fast). With a
handler set, errors are delivered to `OnError` and processing continues.

## Examples

- [Simple Example](example/simple) - Default mapper writing to a generic `documents` table.
- [Struct Config](example/struct-config) - Programmatic config with a custom GORM model and mapper.

## Exposed Metrics

Metric Name                                                                            | Type    | Description                                        | Labels                              |
---------------------------------------------------------------------------------------|---------|----------------------------------------------------|-------------------------------------|
`go_mongo_cdc_postgresql_postgresql_connector_action_total_current`                    | Counter | Count of PostgreSQL actions                        | `action_type` (`upsert`/`delete`/`raw`), `result`, `table_name` |
`go_mongo_cdc_postgresql_postgresql_connector_latency_ms_current`                      | Gauge   | Time to add events to the batch (ms)               | N/A                                 |
`go_mongo_cdc_postgresql_postgresql_connector_batch_process_latency_ms_current`        | Gauge   | Time to process a batch against PostgreSQL (ms)    | N/A                                 |

CDC-related metrics (insert/update/delete counters, checkpoint metrics, CDC
latency, bootstrap counters, etc.) are also exposed by the underlying change
data capture layer on the same Prometheus endpoint. See
[go-mongo-cdc#exposed-metrics](https://github.com/Trendyol/go-mongo-cdc#exposed-metrics).

## Contributing

Go Mongo CDC PostgreSQL is always open for direct contributions. For more information please check our [Contribution Guideline document](CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).

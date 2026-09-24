package main

import (
	"context"
	"time"

	jsoniter "github.com/json-iterator/go"

	cdcpostgresql "github.com/Trendyol/go-mongo-cdc-postgresql"
	"github.com/Trendyol/go-mongo-cdc-postgresql/config"
	"github.com/Trendyol/go-mongo-cdc-postgresql/mongodb"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql"

	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
)

// User is a plain GORM model. GORM derives the table ("users") and primary key
// ("id") from the struct itself.
type User struct {
	ID    string `gorm:"column:id;primaryKey" json:"_id"`
	Name  string `gorm:"column:name"          json:"name"`
	Email string `gorm:"column:email"         json:"email"`
}

// mapper turns each MongoDB event into a typed User upsert/delete.
func mapper(event mongodb.Event) []postgresql.Action {
	if event.IsMutated {
		var u User
		if err := jsoniter.Unmarshal(event.Value, &u); err != nil {
			return nil
		}
		u.ID = event.DocumentID.(string)
		return []postgresql.Action{postgresql.NewUpsertAction(&u)}
	}

	// Delete: only the primary key is required.
	return []postgresql.Action{
		postgresql.NewDeleteAction(&User{ID: event.DocumentID.(string)}),
	}
}

func main() {
	cfg := config.Config{
		PostgreSQL: config.PostgreSQL{
			Host:                "localhost",
			Port:                5432,
			User:                "postgres",
			Password:            "postgres",
			Database:            "mydb",
			SSLMode:             "disable",
			BatchSizeLimit:      1000,
			BatchTickerDuration: 10 * time.Second,
			ConcurrentRequest:   1,
		},
		CDC: cdcConfig.Config{
			MongoDB: cdcConfig.MongoDB{
				Connection: cdcConfig.Connection{
					URI:        "localhost:27017",
					Database:   "exampleDB",
					Collection: "users",
				},
			},
			Checkpoint: cdcConfig.CheckpointConfig{
				TokenSaveInterval:       10 * time.Second,
				BootstrapQueryBatchSize: 5000,
				BootstrapSaveInterval:   10 * time.Second,
			},
			Partition: cdcConfig.PartitionConfig{
				TotalPartition: 1,
				ConsumerGroup:  "postgresql",
			},
			Logger: cdcConfig.LoggerConfig{LogLevel: "info"},
		},
	}

	connector, err := cdcpostgresql.NewConnectorBuilder(cfg).
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}
	defer connector.Close()

	connector.Start(context.Background())
}

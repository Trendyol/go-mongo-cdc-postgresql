package main

import (
	"context"

	cdcpostgresql "github.com/Trendyol/go-mongo-cdc-postgresql"
)

// The simple example uses the DefaultMapper, which writes every document into a
// generic `documents` table (id TEXT PRIMARY KEY, data JSONB).
//
// Create the table before running:
//
//	CREATE TABLE documents (
//	    id   TEXT PRIMARY KEY,
//	    data JSONB NOT NULL
//	);
func main() {
	connector, err := cdcpostgresql.NewConnectorBuilder("config.yml").Build()
	if err != nil {
		panic(err)
	}
	defer connector.Close()

	connector.Start(context.Background())
}

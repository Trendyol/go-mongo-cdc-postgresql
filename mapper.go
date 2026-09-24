package cdcpostgresql

import (
	"github.com/Trendyol/go-mongo-cdc-postgresql/mongodb"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql/document"
)

// Mapper converts a MongoDB change event into zero or more PostgreSQL actions.
type Mapper func(event mongodb.Event) []postgresql.Action

// DefaultMapper writes the full document into the generic `documents` table
// (id TEXT PK, data JSONB) using the document's _id as key. Override it with
// SetMapper to target your own GORM models.
func DefaultMapper(event mongodb.Event) []postgresql.Action {
	id := documentIDToString(event.DocumentID)

	if event.IsMutated {
		return []postgresql.Action{
			postgresql.NewUpsertAction(document.NewDocument(id, event.Value)),
		}
	}

	return []postgresql.Action{
		postgresql.NewDeleteAction(&document.Document{ID: id}),
	}
}

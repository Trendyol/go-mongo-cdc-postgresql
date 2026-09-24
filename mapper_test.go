package cdcpostgresql

import (
	"testing"

	"github.com/Trendyol/go-mongo-cdc-postgresql/mongodb"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql/document"
)

func TestDefaultMapperMutate(t *testing.T) {
	event := mongodb.Event{
		IsMutated:  true,
		DocumentID: "abc",
		Value:      []byte(`{"name":"foo"}`),
	}

	actions := DefaultMapper(event)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Type != postgresql.Upsert {
		t.Fatalf("expected upsert, got %s", actions[0].Type)
	}
	doc, ok := actions[0].Model.(*document.Document)
	if !ok {
		t.Fatalf("expected *document.Document, got %T", actions[0].Model)
	}
	if doc.ID != "abc" {
		t.Fatalf("expected id abc, got %s", doc.ID)
	}
}

func TestDefaultMapperDelete(t *testing.T) {
	event := mongodb.Event{
		IsDeleted:  true,
		DocumentID: "xyz",
	}

	actions := DefaultMapper(event)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Type != postgresql.Delete {
		t.Fatalf("expected delete, got %s", actions[0].Type)
	}
}

func TestDocumentIDToString(t *testing.T) {
	if got := documentIDToString("id1"); got != "id1" {
		t.Fatalf("expected id1, got %s", got)
	}
	if got := documentIDToString(42); got != "42" {
		t.Fatalf("expected 42, got %s", got)
	}
	if got := documentIDToString(int64(7)); got != "7" {
		t.Fatalf("expected 7, got %s", got)
	}
}

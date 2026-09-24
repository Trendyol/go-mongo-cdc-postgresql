package document

import "gorm.io/datatypes"

// Document is a generic, schema-less GORM model used by the DefaultMapper. It
// stores the MongoDB _id and the full document as JSONB. Use it when you don't
// want to declare a dedicated struct per collection.
//
// Expected table schema:
//
//	CREATE TABLE documents (
//	    id   TEXT PRIMARY KEY,
//	    data JSONB NOT NULL
//	);
type Document struct {
	ID   string         `gorm:"column:id;primaryKey"`
	Data datatypes.JSON `gorm:"column:data;type:jsonb"`
}

func (Document) TableName() string {
	return "documents"
}

// NewDocument builds a generic Document from an id and raw JSON value.
func NewDocument(id string, value []byte) *Document {
	if len(value) == 0 {
		value = []byte("{}")
	}
	return &Document{
		ID:   id,
		Data: datatypes.JSON(value),
	}
}

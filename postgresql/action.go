package postgresql

// ActionType is the type of a PostgreSQL sink action.
type ActionType string

const (
	// Upsert inserts the model or updates it on primary key / unique constraint conflict.
	Upsert ActionType = "Upsert"

	// Delete removes the model by its primary key.
	Delete ActionType = "Delete"

	// Raw executes an arbitrary SQL statement with arguments.
	Raw ActionType = "Raw"
)

// Action describes a single change to be applied to PostgreSQL via GORM.
//
// For Upsert/Delete, Model is any GORM model (a struct pointer, e.g. *User).
// GORM derives the target table and primary key from the model itself.
//
// For Raw, set RawQuery (and optionally RawArgs); GORM executes it verbatim.
type Action struct {
	// Model is the GORM model instance for this action.
	Model any

	// Type is either Upsert, Delete or Raw.
	Type ActionType

	// TableName optionally overrides the table GORM would infer from Model.
	// For Raw actions, set TableName so the statement is grouped and executed
	// in arrival order with that table's other actions.
	TableName string

	// RawQuery is the SQL statement executed for Raw actions.
	RawQuery string

	// RawArgs are the bound arguments of RawQuery.
	RawArgs []any
}

// NewUpsertAction creates an upsert action for the given fully-populated model.
func NewUpsertAction(model any) Action {
	return Action{
		Model: model,
		Type:  Upsert,
	}
}

// NewUpsertActionWithTable creates an upsert action targeting an explicit table.
func NewUpsertActionWithTable(tableName string, model any) Action {
	return Action{
		Model:     model,
		Type:      Upsert,
		TableName: tableName,
	}
}

// NewDeleteAction creates a delete action. The model only needs its primary key set.
func NewDeleteAction(model any) Action {
	return Action{
		Model: model,
		Type:  Delete,
	}
}

// NewDeleteActionWithTable creates a delete action targeting an explicit table.
func NewDeleteActionWithTable(tableName string, model any) Action {
	return Action{
		Model:     model,
		Type:      Delete,
		TableName: tableName,
	}
}

// NewRawAction creates an action that executes an arbitrary SQL statement.
func NewRawAction(query string, args ...any) Action {
	return Action{
		Type:     Raw,
		RawQuery: query,
		RawArgs:  args,
	}
}

// NewRawActionWithTable creates a raw action associated with a table so it is
// grouped and executed in arrival order with that table's other actions.
func NewRawActionWithTable(tableName, query string, args ...any) Action {
	return Action{
		Type:      Raw,
		TableName: tableName,
		RawQuery:  query,
		RawArgs:   args,
	}
}

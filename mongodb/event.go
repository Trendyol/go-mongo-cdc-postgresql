package mongodb

import "time"

// Event represents a single MongoDB change event that is passed to the mapper.
type Event struct {
	// EventTime is the cluster time of the change event.
	EventTime time.Time

	// CollectionName is the source MongoDB collection.
	CollectionName string

	// DocumentID is the raw MongoDB document identifier (_id) as received from the change stream.
	DocumentID any

	// Value is the full document (excluding _id) marshalled to JSON, populated for mutate events.
	Value []byte

	// FullDocument is the raw decoded document map, populated for mutate events.
	FullDocument map[string]any

	// IsDeleted is true for delete operations.
	IsDeleted bool

	// IsMutated is true for insert/update/replace operations.
	IsMutated bool

	// PartitionID is the CDC partition that produced this event.
	PartitionID int
}

func NewDeleteEvent(
	documentID any,
	collectionName string,
	eventTime time.Time,
	partitionID int,
) Event {
	return Event{
		DocumentID:     documentID,
		IsDeleted:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		PartitionID:    partitionID,
	}
}

func NewMutateEvent(
	documentID any,
	value []byte,
	fullDocument map[string]any,
	collectionName string,
	eventTime time.Time,
	partitionID int,
) Event {
	return Event{
		DocumentID:     documentID,
		Value:          value,
		FullDocument:   fullDocument,
		IsMutated:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		PartitionID:    partitionID,
	}
}

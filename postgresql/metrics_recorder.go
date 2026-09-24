package postgresql

type MetricsRecorder interface {
	RecordUpsertSuccess(tableName string, count int64)
	RecordUpsertError(tableName string, count int64)
	RecordDeleteSuccess(tableName string, count int64)
	RecordDeleteError(tableName string, count int64)
	RecordRawSuccess(tableName string, count int64)
	RecordRawError(tableName string, count int64)
	RecordProcessLatency(latencyMs int64)
	RecordBatchProcessLatency(latencyMs int64)
}

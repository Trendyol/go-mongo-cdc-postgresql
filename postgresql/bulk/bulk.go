package bulk

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-mongo-cdc-postgresql/config"
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/stream"
	"golang.org/x/sync/errgroup"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type Bulk struct {
	sinkResponseHandler          postgresql.SinkResponseHandler
	metricsRecorder              postgresql.MetricsRecorder
	db                           *gorm.DB
	config                       *config.Config
	batchKeys                    map[string]int
	cdcCheckpointCommit          func()
	cdcCheckpointCommitBootstrap func(partitionID int)
	batchTicker                  *time.Ticker
	batchCommitTicker            *time.Ticker
	rebalancingPartitions        map[int]bool
	batch                        []BatchItem
	schemaCache                  sync.Map
	rawKeyCounter                atomic.Uint64
	batchIndex                   int
	batchSize                    int
	batchSizeLimit               int
	batchTickerDuration          time.Duration
	concurrentRequest            int
	statementTimeout             time.Duration
	flushLock                    sync.Mutex
}

// insertBatchSize is the number of rows GORM sends per multi-row INSERT to stay
// well under PostgreSQL's 65535 bind-parameter limit regardless of column count.
const insertBatchSize = 500

// rawTableName is the internal grouping bucket for raw actions without TableName.
const rawTableName = "__raw__"

type BatchItem struct {
	Action      *postgresql.Action
	PartitionID int
	IsBootstrap bool
}

func NewBulk(
	cfg *config.Config,
	cdcCheckpointCommit func(),
	cdcCheckpointCommitBootstrap func(partitionID int),
	db *gorm.DB,
	sinkResponseHandler postgresql.SinkResponseHandler,
	metricsRecorder postgresql.MetricsRecorder,
) (*Bulk, error) {
	b := &Bulk{
		batchTickerDuration:          cfg.PostgreSQL.BatchTickerDuration,
		batchTicker:                  time.NewTicker(cfg.PostgreSQL.BatchTickerDuration),
		batchSizeLimit:               cfg.PostgreSQL.BatchSizeLimit,
		cdcCheckpointCommit:          cdcCheckpointCommit,
		cdcCheckpointCommitBootstrap: cdcCheckpointCommitBootstrap,
		db:                           db,
		metricsRecorder:              metricsRecorder,
		config:                       cfg,
		concurrentRequest:            cfg.PostgreSQL.ConcurrentRequest,
		statementTimeout:             cfg.PostgreSQL.StatementTimeout,
		batchKeys:                    make(map[string]int, cfg.PostgreSQL.BatchSizeLimit),
		sinkResponseHandler:          sinkResponseHandler,
		rebalancingPartitions:        make(map[int]bool),
	}

	if cfg.PostgreSQL.BatchCommitTickerDuration != nil {
		b.batchCommitTicker = time.NewTicker(*cfg.PostgreSQL.BatchCommitTickerDuration)
	}

	if sinkResponseHandler != nil {
		sinkResponseHandler.OnInit(&postgresql.SinkResponseHandlerInitContext{
			Config: cfg,
			DB:     db,
		})
	}

	return b, nil
}

func (b *Bulk) StartBulk() {
	for range b.batchTicker.C {
		b.flushMessages()
	}
}

func (b *Bulk) AddActions(
	ctx *stream.ListenerContext,
	eventTime time.Time,
	actions []postgresql.Action,
	isLastChunk bool,
	partitionID int,
	isBootstrap bool,
) {
	b.flushLock.Lock()
	if b.rebalancingPartitions[partitionID] {
		logger.Log.Debug("could not add new action to batch while partition is rebalancing - partitionId: %d", partitionID)
		b.flushLock.Unlock()
		return
	}

	for i := range actions {
		action := actions[i]
		key := b.getActionKey(action)

		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batch[batchIndex] = BatchItem{
				Action:      &actions[i],
				PartitionID: partitionID,
				IsBootstrap: isBootstrap,
			}
		} else {
			b.batch = append(b.batch, BatchItem{
				Action:      &actions[i],
				PartitionID: partitionID,
				IsBootstrap: isBootstrap,
			})
			b.batchKeys[key] = b.batchIndex
			b.batchIndex++
			b.batchSize++
		}
	}

	if isLastChunk {
		ctx.Ack()
	}

	shouldFlush := b.batchSize >= b.batchSizeLimit

	b.flushLock.Unlock()

	if isLastChunk {
		b.metricsRecorder.RecordProcessLatency(time.Since(eventTime).Milliseconds())
	}

	if shouldFlush {
		b.flushMessages()
	}
}

func (b *Bulk) Close() {
	b.batchTicker.Stop()
	if b.batchCommitTicker != nil {
		b.batchCommitTicker.Stop()
	}
	b.flushMessages()
}

func (b *Bulk) PreparePartitionRebalancing(partitionID int) {
	b.flushLock.Lock()
	b.rebalancingPartitions[partitionID] = true
	b.flushLock.Unlock()
}

func (b *Bulk) EndPartitionRebalancing(partitionID int) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	delete(b.rebalancingPartitions, partitionID)
}

func (b *Bulk) FlushMessages() {
	b.flushMessages()
}

func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if len(b.batch) > 0 {
		err := b.executeBatch()
		if err != nil && b.sinkResponseHandler == nil {
			logger.Log.Error("error while executing postgresql batch, err: %v", err)
			panic(err)
		}

		b.batchTicker.Reset(b.batchTickerDuration)

		bootstrapPartitions := make(map[int]bool)
		for _, item := range b.batch {
			if item.IsBootstrap {
				bootstrapPartitions[item.PartitionID] = true
			}
		}

		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0

		b.checkAndCommit(bootstrapPartitions)
	} else {
		b.checkAndCommit(nil)
	}
}

func (b *Bulk) checkAndCommit(bootstrapPartitions map[int]bool) {
	if b.batchCommitTicker == nil {
		b.cdcCheckpointCommit()
		b.commitBootstrapCheckpoints(bootstrapPartitions)
		return
	}

	select {
	case <-b.batchCommitTicker.C:
		b.cdcCheckpointCommit()
		b.commitBootstrapCheckpoints(bootstrapPartitions)
	default:
		return
	}
}

func (b *Bulk) commitBootstrapCheckpoints(bootstrapPartitions map[int]bool) {
	if bootstrapPartitions == nil {
		return
	}
	for partitionID := range bootstrapPartitions {
		b.cdcCheckpointCommitBootstrap(partitionID)
	}
}

// statement is a group of consecutive, compatible actions that can be executed
// as a single SQL statement.
type statement struct {
	kind      postgresql.ActionType
	tableName string
	modelType reflect.Type
	items     []*postgresql.Action
}

func (b *Bulk) executeBatch() error {
	startedTime := time.Now()
	defer func() {
		b.metricsRecorder.RecordBatchProcessLatency(time.Since(startedTime).Milliseconds())
	}()

	tableGroups := b.groupByTable()

	eg, _ := errgroup.WithContext(context.Background())
	eg.SetLimit(b.concurrentRequest)

	for i := range tableGroups {
		tg := tableGroups[i]
		eg.Go(func() error {
			return b.executeTableGroup(tg)
		})
	}

	return eg.Wait()
}

// groupByTable buckets batch items by target table while preserving arrival
// order within each bucket. Actions on different tables run concurrently;
// actions on the same table (including Raw actions tagged with that table) run
// strictly in arrival order, so upserts and deletes of the same row never race.
func (b *Bulk) groupByTable() [][]*postgresql.Action {
	groups := make([][]*postgresql.Action, 0)
	index := make(map[string]int)

	for _, item := range b.batch {
		action := item.Action
		table := b.tableGroupKey(action)

		idx, ok := index[table]
		if !ok {
			idx = len(groups)
			index[table] = idx
			groups = append(groups, nil)
		}
		groups[idx] = append(groups[idx], action)
	}

	return groups
}

func (b *Bulk) tableGroupKey(action *postgresql.Action) string {
	if action.Type == postgresql.Raw {
		if action.TableName != "" {
			return action.TableName
		}
		return rawTableName
	}
	return b.resolveTableName(action)
}

// executeTableGroup converts the ordered action list into minimal statements:
// consecutive compatible upserts/deletes are merged into one multi-row
// statement; Raw actions execute verbatim at their position.
func (b *Bulk) executeTableGroup(actions []*postgresql.Action) error {
	var firstErr error
	var pending *statement

	flushPending := func() {
		if pending == nil {
			return
		}
		if err := b.executeStatement(pending); err != nil && firstErr == nil {
			firstErr = err
		}
		pending = nil
	}

	for _, action := range actions {
		if action.Type == postgresql.Raw {
			flushPending()
			if err := b.executeStatement(rawStatement(action)); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}

		modelType := reflect.TypeOf(action.Model)
		if pending != nil &&
			pending.kind == action.Type &&
			pending.modelType == modelType {
			pending.items = append(pending.items, action)
			continue
		}

		flushPending()
		pending = &statement{
			kind:      action.Type,
			tableName: action.TableName,
			modelType: modelType,
			items:     []*postgresql.Action{action},
		}
	}
	flushPending()

	return firstErr
}

func rawStatement(action *postgresql.Action) *statement {
	return &statement{
		kind:      postgresql.Raw,
		tableName: action.TableName,
		items:     []*postgresql.Action{action},
	}
}

func (b *Bulk) executeStatement(s *statement) error {
	tx := b.db.Session(&gorm.Session{})
	if b.statementTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), b.statementTimeout)
		defer cancel()
		tx = tx.WithContext(ctx)
	}
	if s.tableName != "" && s.tableName != rawTableName {
		tx = tx.Table(s.tableName)
	}

	var err error
	switch s.kind {
	case postgresql.Raw:
		action := s.items[0]
		err = tx.Exec(action.RawQuery, action.RawArgs...).Error

	case postgresql.Upsert:
		models := buildModelSlice(s)
		err = tx.Clauses(clause.OnConflict{UpdateAll: true}).
			CreateInBatches(models.Interface(), insertBatchSize).Error

	case postgresql.Delete:
		models := buildModelSlice(s)
		err = tx.Delete(models.Interface()).Error

	default:
		err = fmt.Errorf("unsupported action type: %s", s.kind)
	}

	b.finalizeStatement(s, err)
	return err
}

// buildModelSlice creates a typed slice ([]*T) from the statement's actions so
// GORM can infer schema and execute a batched statement.
func buildModelSlice(s *statement) reflect.Value {
	sliceType := reflect.SliceOf(s.modelType)
	slice := reflect.MakeSlice(sliceType, 0, len(s.items))
	for _, item := range s.items {
		slice = reflect.Append(slice, reflect.ValueOf(item.Model))
	}
	ptr := reflect.New(sliceType)
	ptr.Elem().Set(slice)
	return ptr
}

func (b *Bulk) finalizeStatement(s *statement, err error) {
	for _, action := range s.items {
		tableName := b.metricsTableName(action)
		if err != nil {
			b.countError(action.Type, tableName)
			if b.sinkResponseHandler != nil {
				b.sinkResponseHandler.OnError(&postgresql.SinkResponseHandlerContext{
					Action: action,
					Err:    err,
				})
			}
		} else {
			b.countSuccess(action.Type, tableName)
			if b.sinkResponseHandler != nil {
				b.sinkResponseHandler.OnSuccess(&postgresql.SinkResponseHandlerContext{
					Action: action,
				})
			}
		}
	}
}

func (b *Bulk) metricsTableName(action *postgresql.Action) string {
	if action.TableName != "" {
		return action.TableName
	}
	if action.Type == postgresql.Raw {
		return "(raw)"
	}
	return b.resolveTableName(action)
}

func (b *Bulk) countSuccess(actionType postgresql.ActionType, tableName string) {
	switch actionType {
	case postgresql.Upsert:
		b.metricsRecorder.RecordUpsertSuccess(tableName, 1)
	case postgresql.Delete:
		b.metricsRecorder.RecordDeleteSuccess(tableName, 1)
	case postgresql.Raw:
		b.metricsRecorder.RecordRawSuccess(tableName, 1)
	}
}

func (b *Bulk) countError(actionType postgresql.ActionType, tableName string) {
	switch actionType {
	case postgresql.Upsert:
		b.metricsRecorder.RecordUpsertError(tableName, 1)
	case postgresql.Delete:
		b.metricsRecorder.RecordDeleteError(tableName, 1)
	case postgresql.Raw:
		b.metricsRecorder.RecordRawError(tableName, 1)
	}
}

// getActionKey builds a unique key for an action so newer changes to the same
// row within one batch overwrite older ones. Raw actions never deduplicate.
func (b *Bulk) getActionKey(action postgresql.Action) string {
	if action.Type == postgresql.Raw {
		return fmt.Sprintf("raw:%d", b.rawKeyCounter.Add(1))
	}

	tableName := b.tableGroupKey(&action)
	pk := b.primaryKeyValue(action.Model)
	return fmt.Sprintf("%s:%s:%s", action.Type, tableName, pk)
}

func (b *Bulk) resolveTableName(action *postgresql.Action) string {
	if action.TableName != "" {
		return action.TableName
	}

	s := b.parseSchema(action.Model)
	if s != nil {
		return s.Table
	}
	return reflect.TypeOf(action.Model).String()
}

func (b *Bulk) primaryKeyValue(model any) string {
	s := b.parseSchema(model)
	if s == nil || len(s.PrimaryFields) == 0 {
		return fmt.Sprintf("%v", model)
	}

	rv := reflect.Indirect(reflect.ValueOf(model))
	if !rv.IsValid() {
		return fmt.Sprintf("%v", model)
	}

	parts := make([]string, 0, len(s.PrimaryFields))
	for _, pf := range s.PrimaryFields {
		fv := rv
		for _, idx := range pf.StructField.Index {
			if fv.Kind() == reflect.Ptr {
				if fv.IsNil() {
					fv = reflect.Value{}
					break
				}
				fv = fv.Elem()
			}
			fv = fv.Field(idx)
		}
		if fv.IsValid() {
			parts = append(parts, fmt.Sprintf("%v", fv.Interface()))
		}
	}
	return fmt.Sprintf("%v", parts)
}

func (b *Bulk) parseSchema(model any) *schema.Schema {
	t := reflect.TypeOf(model)
	if cached, ok := b.schemaCache.Load(t); ok {
		if s, ok := cached.(*schema.Schema); ok {
			return s
		}
		return nil
	}

	s, err := schema.Parse(model, &sync.Map{}, b.db.NamingStrategy)
	if err != nil {
		logger.Log.Warn("failed to parse schema for model %T: %v", model, err)
		b.schemaCache.Store(t, (*schema.Schema)(nil))
		return nil
	}
	b.schemaCache.Store(t, s)
	return s
}

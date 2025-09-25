package sinks

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
)

const MongoSink = "mongo-sink"

const (
	ERR_MONGO_SINK_NIL               = "mongo sink is nil"
	ERR_MONGO_SINK_NIL_CLIENT        = "mongo sink: nil client"
	ERR_MONGO_SINK_EMPTY_COLL        = "mongo sink: empty collection"
	ERR_MONGO_SINK_EMPTY_DATA        = "mongo sink: empty data, nothing to write"
	ERR_MONGO_SINK_DB_PROTOCOL       = "mongo sink: DB protocol is required"
	ERR_MONGO_SINK_DB_HOST           = "mongo sink: DB host is required"
	ERR_MONGO_SINK_DB_NAME           = "mongo sink: DB name is required"
	ERR_MONGO_SINK_DB_USER           = "mongo sink: DB user is required"
	ERR_MONGO_SINK_DB_PWD            = "mongo sink: DB password is required"
	ERR_MONGO_SINK_ALL_BATCH_RECORDS = "mongo sink: all batch records failed"
)

var (
	ErrMongoSinkNil             = errors.New(ERR_MONGO_SINK_NIL)
	ErrMongoSinkNilClient       = errors.New(ERR_MONGO_SINK_NIL_CLIENT)
	ErrMongoSinkEmptyColl       = errors.New(ERR_MONGO_SINK_EMPTY_COLL)
	ErrMongoSinkEmptyData       = errors.New(ERR_MONGO_SINK_EMPTY_DATA)
	ErrMongoSinkDBProtocol      = errors.New(ERR_MONGO_SINK_DB_PROTOCOL)
	ErrMongoSinkDBHost          = errors.New(ERR_MONGO_SINK_DB_HOST)
	ErrMongoSinkDBName          = errors.New(ERR_MONGO_SINK_DB_NAME)
	ErrMongoSinkDBUser          = errors.New(ERR_MONGO_SINK_DB_USER)
	ErrMongoSinkDBPwd           = errors.New(ERR_MONGO_SINK_DB_PWD)
	ErrMongoSinkAllBatchRecords = errors.New(ERR_MONGO_SINK_ALL_BATCH_RECORDS)
)

// MongoDocWriter is the tiny capability we need.
type MongoDocWriter interface {
	AddCollectionDoc(ctx context.Context, collectionName string, doc map[string]any) (string, error)
	Stats(ctx context.Context, db string)
	Close(ctx context.Context) error
}

// MongoDB sink.
type mongoSink[T any] struct {
	client     MongoDocWriter // MongoDB client
	collection string         // collection name
	dbName     string         // database name
}

// Name returns the name of the mongo sink.
func (s *mongoSink[T]) Name() string { return MongoSink }

// Write writes the batch of records to MongoDB.
func (s *mongoSink[T]) Write(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}
	if s == nil {
		return b, ErrMongoSinkNil
	}
	if s.client == nil {
		return b, ErrMongoSinkNilClient
	}
	if s.collection == "" {
		return b, ErrMongoSinkEmptyColl
	}

	if len(b.Records) == 0 {
		return b, ErrMongoSinkEmptyData
	}

	var errCount int
	var errs map[string]int
	for i, rec := range b.Records {
		// allow cancellation
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		default:
		}

		if rec.BatchResult.Error != "" {
			// TODO Differentiate between transient and permanent errors
			l.Debug(
				"skipping record with existing error",
				"batch-id", b.BatchId,
				"rec-start", rec.Start,
				"rec-end", rec.End,
				"error", rec.BatchResult.Error)
			continue // skip already errored records
		}

		doc, err := toMapAny(rec.Data)
		if err != nil {
			b.Records[i].BatchResult.Error = err.Error()
			if errs == nil {
				errs = make(map[string]int)
			}
			errs[err.Error()]++
			errCount++
			continue
		}

		res, err := s.client.AddCollectionDoc(ctx, s.collection, doc)
		if err != nil {
			b.Records[i].BatchResult.Error = err.Error()
			// skip duplicate record errors
			if err != mongostore.ErrDuplicateRecord {
				if errs == nil {
					errs = make(map[string]int)
				}
				errs[err.Error()]++
				errCount++
			}
			continue
		}
		b.Records[i].BatchResult.Result = res // store the inserted ID or result
	}

	// Set the error map on the batch
	b.Error = errs

	if time.Now().Unix()%109 == 0 {
		// check for mongo cluster status
		// s.client.Stats(ctx, s.dbName)
	}

	if errCount >= len(b.Records) {
		l.Error("all batch records failed", "batch-id", b.BatchId, "errors", errs)
		return b, ErrMongoSinkAllBatchRecords
	}
	return b, nil
}

// WriteStream writes the batch of records to MongoDB.
func (s *mongoSink[T]) WriteStream(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}
	if s == nil {
		return b, ErrMongoSinkNil
	}
	if s.client == nil {
		return b, ErrMongoSinkNilClient
	}
	if s.collection == "" {
		return b, ErrMongoSinkEmptyColl
	}

	if len(b.Records) == 0 {
		return b, ErrMongoSinkEmptyData
	}

	type out struct {
		idx int
		res string
		err error
	}

	const maxConcurrent = 8
	sem := make(chan struct{}, maxConcurrent) // semaphore to cap concurrency
	ch := make(chan out)
	var wg sync.WaitGroup
	launched := 0

	var errCount int
	errs := map[string]int{}
	for i, rec := range b.Records {
		// allow cancellation before launching work
		select {
		case <-ctx.Done():
			// Stop launching new work; we'll still drain anything already launched.
			goto waitAndCollect
		default:
		}

		if rec.BatchResult.Error != "" {
			// TODO Differentiate between transient and permanent errors
			l.Debug(
				"skipping record with existing error",
				"batch-id", b.BatchId,
				"rec-start", rec.Start,
				"rec-end", rec.End,
				"error", rec.BatchResult.Error)
			continue // skip already errored records
		}

		launched++
		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			doc, err := toMapAny(rec.Data)
			if err != nil {
				select {
				case ch <- out{idx: i, err: err}:
				case <-ctx.Done():
				}
				return
			}

			res, err := s.client.AddCollectionDoc(ctx, s.collection, doc)
			if err != nil {
				select {
				case ch <- out{idx: i, err: err}:
				case <-ctx.Done():
				}
				return
			}

			select {
			case ch <- out{idx: i, res: res}:
			case <-ctx.Done():
			}
		}(i)
	}

waitAndCollect:
	// Close ch after all launched goroutines finish.
	go func() {
		wg.Wait()
		close(ch)
	}()

	// Collect results and update b in a single goroutine (this one).
	for r := range ch {
		if r.err != nil {
			b.Records[r.idx].BatchResult.Error = r.err.Error()
			// skip duplicate record errors
			if r.err != mongostore.ErrDuplicateRecord {
				errs[r.err.Error()]++
				errCount++
			}
			continue
		}
		b.Records[r.idx].BatchResult.Result = r.res
	}

	// Set the error map on the batch
	b.Error = errs

	// If we aborted launches due to cancellation, surface that.
	if err := ctx.Err(); err != nil {
		l.Error("context error", "batch-id", b.BatchId, "error", err)
		return b, err
	}

	if errCount >= len(b.Records) {
		l.Error("all batch records failed", "batch-id", b.BatchId, "errors", errs)
		return b, ErrMongoSinkAllBatchRecords
	}
	return b, nil
}

// Close closes the mongo sink.
func (s *mongoSink[T]) Close(ctx context.Context) error {
	return s.client.Close(ctx)
}

// MongoDB sink config.
type MongoSinkConfig[T any] struct {
	Protocol   string // e.g., "mongodb", "mongodb+srv"
	Host       string // e.g., "localhost:27017"
	DBName     string // e.g., "testdb"
	User       string // MongoDB user
	Pwd        string // MongoDB password
	Params     string // e.g., "retryWrites=true&w=majority"
	Collection string
}

// Name of the sink.
func (c *MongoSinkConfig[T]) Name() string { return MongoSink }

// BuildSink builds a MongoDB sink from the config.
func (c *MongoSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if c.Protocol == "" {
		return nil, ErrMongoSinkDBProtocol
	}
	if c.Host == "" {
		return nil, ErrMongoSinkDBHost
	}
	if c.DBName == "" {
		return nil, ErrMongoSinkDBName
	}
	if c.User == "" {
		return nil, ErrMongoSinkDBUser
	}
	if c.Pwd == "" {
		return nil, ErrMongoSinkDBPwd
	}
	if c.Collection == "" {
		return nil, ErrMongoSinkEmptyColl
	}

	mCfg := mongostore.NewMongoDBConfig(
		c.Protocol,
		c.Host,
		c.User,
		c.Pwd,
		c.Params,
		c.DBName,
	)

	mCl, err := mongostore.NewMongoStore(ctx, mCfg)
	if err != nil {
		l.Error("error creating mongo store", "error", err.Error())
		return nil, err
	}

	// init client/collection; consider pooling in activities
	return &mongoSink[T]{
		client:     mCl,
		collection: c.Collection,
		dbName:     c.DBName,
	}, nil
}

// toMapAny converts common row shapes to map[string]any.
//   - map[string]any: pass-through
//   - map[string]string: widen to any
//   - everything else: JSON round-trip into map[string]any
func toMapAny[T any](rec T) (map[string]any, error) {
	// Fast paths
	if m, ok := any(rec).(map[string]any); ok {
		return m, nil
	}
	if ms, ok := any(rec).(map[string]string); ok {
		out := make(map[string]any, len(ms))
		for k, v := range ms {
			out[k] = v
		}
		return out, nil
	}

	// Fallback: JSON round-trip (covers structs, slices, etc.)
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

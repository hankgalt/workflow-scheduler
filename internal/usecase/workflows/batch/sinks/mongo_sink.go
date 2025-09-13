package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hankgalt/batch-orchestra/pkg/domain"

	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
)

const MongoSink = "mongo-sink"

const (
	ERR_MONGO_SINK_NIL         = "mongo sink is nil"
	ERR_MONGO_SINK_NIL_CLIENT  = "mongo sink: nil client"
	ERR_MONGO_SINK_EMPTY_COLL  = "mongo sink: empty collection"
	ERR_MONGO_SINK_EMPTY_DATA  = "mongo sink: empty data, nothing to write"
	ERR_MONGO_SINK_DB_PROTOCOL = "mongo sink: DB protocol is required"
	ERR_MONGO_SINK_DB_HOST     = "mongo sink: DB host is required"
	ERR_MONGO_SINK_DB_NAME     = "mongo sink: DB name is required"
	ERR_MONGO_SINK_DB_USER     = "mongo sink: DB user is required"
	ERR_MONGO_SINK_DB_PWD      = "mongo sink: DB password is required"
)

var (
	ErrMongoSinkNil        = errors.New(ERR_MONGO_SINK_NIL)
	ErrMongoSinkNilClient  = errors.New(ERR_MONGO_SINK_NIL_CLIENT)
	ErrMongoSinkEmptyColl  = errors.New(ERR_MONGO_SINK_EMPTY_COLL)
	ErrMongoSinkEmptyData  = errors.New(ERR_MONGO_SINK_EMPTY_DATA)
	ErrMongoSinkDBProtocol = errors.New(ERR_MONGO_SINK_DB_PROTOCOL)
	ErrMongoSinkDBHost     = errors.New(ERR_MONGO_SINK_DB_HOST)
	ErrMongoSinkDBName     = errors.New(ERR_MONGO_SINK_DB_NAME)
	ErrMongoSinkDBUser     = errors.New(ERR_MONGO_SINK_DB_USER)
	ErrMongoSinkDBPwd      = errors.New(ERR_MONGO_SINK_DB_PWD)
)

// MongoDocWriter is the tiny capability we need.
type MongoDocWriter interface {
	AddCollectionDoc(ctx context.Context, collectionName string, doc map[string]any) (string, error)
	Close(ctx context.Context) error
}

// MongoDB sink.
type mongoSink[T any] struct {
	client     MongoDocWriter // MongoDB client
	collection string         // collection name
}

// Name returns the name of the mongo sink.
func (s *mongoSink[T]) Name() string { return MongoSink }

// Write writes the batch of records to MongoDB.
func (s *mongoSink[T]) Write(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
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

	for i, rec := range b.Records {
		// allow cancellation
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		default:
		}

		if rec.BatchResult.Error != "" {
			continue // skip already errored records
		}

		doc, err := toMapAny(rec.Data)
		if err != nil {
			b.Records[i].BatchResult.Error = fmt.Sprintf("record %d convert: %s", i, err.Error())
			continue
		}

		res, err := s.client.AddCollectionDoc(ctx, s.collection, doc)
		if err != nil {
			b.Records[i].BatchResult.Error = fmt.Sprintf("record %d insert: %s", i, err.Error())
			continue
		}
		b.Records[i].BatchResult.Result = res // store the inserted ID or result
	}
	return b, nil
}

func (s *mongoSink[T]) WriteStream(ctx context.Context, start uint64, data []T) (<-chan domain.BatchResult, error) {
	if s == nil {
		return nil, ErrMongoSinkNil
	}
	if s.client == nil {
		return nil, ErrMongoSinkNilClient
	}
	if s.collection == "" {
		return nil, ErrMongoSinkEmptyColl
	}

	if len(data) == 0 {
		return nil, ErrMongoSinkEmptyData
	}

	resStream := make(chan domain.BatchResult)

	go func() {
		defer close(resStream)

		for i, rec := range data {
			// allow cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			doc, err := toMapAny(rec)
			if err != nil {
				resStream <- domain.BatchResult{
					Error: fmt.Sprintf("record %d convert: %s", i, err.Error()),
				}
				continue
			}

			res, err := s.client.AddCollectionDoc(ctx, s.collection, doc)
			if err != nil {
				resStream <- domain.BatchResult{Error: fmt.Sprintf("record %d insert: %s", i, err.Error())}
				continue
			}
			resStream <- domain.BatchResult{
				Result: res,
			}
		}
	}()

	return resStream, nil
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
func (c MongoSinkConfig[T]) Name() string { return MongoSink }

// BuildSink builds a MongoDB sink from the config.
func (c MongoSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
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
		return nil, fmt.Errorf("mongo sink: create store: %w", err)
	}

	// init client/collection; consider pooling in activities
	return &mongoSink[T]{
		client:     mCl,
		collection: c.Collection,
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
		return nil, fmt.Errorf("marshal: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return out, nil
}

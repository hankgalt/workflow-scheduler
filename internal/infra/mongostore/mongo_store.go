package mongostore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/infra"
)

const (
	ERR_MISSING_DB_NAME      = "missing database name"
	ERR_MONGO_CLIENT_CONN    = "error connecting with mongo client"
	ERR_MONGO_CLIENT_DISCONN = "error disconnecting with mongo client"
)

var (
	ErrMissingDBName      = errors.New(ERR_MISSING_DB_NAME)
	ErrMongoClientConn    = errors.New(ERR_MONGO_CLIENT_CONN)
	ErrMongoClientDisconn = errors.New(ERR_MONGO_CLIENT_DISCONN)
)

type mongoStore struct {
	client *mongo.Client
	store  *mongo.Database
}

func NewMongoStore(ctx context.Context, cfg infra.StoreConfig) (*mongoStore, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("NewMongoStore - error getting logger from context: %w", err)
	}

	builder := NewMongoConnectionBuilder(cfg.Protocol(), cfg.Host()).WithUser(cfg.User()).WithPassword(cfg.Pwd()).WithConnectionParams(cfg.Params())
	opts := &MongoStoreOption{
		ClientOption: infra.DefaultClientOption(),
		DBName:       cfg.Name(), // Database name is required for MongoDB operations
		PoolSize:     10,         // Default pool size, can be adjusted based on application needs
	}

	if opts.DBName == "" {
		return nil, ErrMissingDBName
	}

	dbConnStr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	mOpts := options.Client().ApplyURI(dbConnStr).SetReadPreference(readpref.Primary()).SetAppName(opts.Caller).SetMaxPoolSize(opts.PoolSize)

	cl, err := mongo.Connect(ctx, mOpts)
	if err != nil {
		l.Error("error connecting to MongoDB", "error", err.Error())
		return nil, ErrMongoClientConn
	}

	if err = cl.Ping(ctx, nil); err != nil {
		if disconnectErr := cl.Disconnect(ctx); disconnectErr != nil {
			l.Error("error disconnecting from MongoDB", "error", disconnectErr.Error())
			return nil, errors.Join(ErrMongoClientConn, ErrMongoClientDisconn)
		}
		return nil, ErrMongoClientConn
	}

	return &mongoStore{
		client: cl,
		store:  cl.Database(opts.DBName),
	}, nil
}

func (ms *mongoStore) EnsureIndexes(ctx context.Context, collectionName string, indexes []mongo.IndexModel) error {
	collection := ms.store.Collection(collectionName)
	_, err := collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create indexes on collection %q: %w", collectionName, err)
	}
	return nil
}

func (ms *mongoStore) Store() *mongo.Database {
	return ms.store
}

func (ms *mongoStore) Close(ctx context.Context) error {
	if err := ms.client.Disconnect(ctx); err != nil {
		return ErrMongoClientDisconn
	}
	return nil
}

func (ms *mongoStore) Stats(ctx context.Context, db string) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return
	}

	var stats infra.ConnPoolStats
	// var stats map[string]interface{}
	err = ms.client.Database(db).RunCommand(ctx, bson.D{{Key: "connPoolStats", Value: 1}}).Decode(&stats)
	if err != nil {
		l.Error("stats error", slog.String("error", err.Error()))
	}

	l.Debug("mongo client stats", slog.Any("stats", stats))
}

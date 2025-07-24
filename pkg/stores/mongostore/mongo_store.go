package mongostore

import (
	"context"
	"errors"
	"log/slog"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
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

func NewMongoStore(ctx context.Context, opts *MongoStoreOption, mcb ConnectionBuilder) (*mongoStore, error) {
	if opts.DBName == "" {
		return nil, ErrMissingDBName
	}

	dbConnStr, err := mcb.Build()
	if err != nil {
		return nil, err
	}

	mOpts := options.Client().ApplyURI(dbConnStr).SetAppName(opts.Caller).SetMaxPoolSize(opts.PoolSize)

	cl, err := mongo.Connect(ctx, mOpts)
	if err != nil {
		return nil, ErrMongoClientConn
	}

	if err = cl.Ping(ctx, nil); err != nil {
		if disconnectErr := cl.Disconnect(ctx); disconnectErr != nil {
			return nil, errors.Join(ErrMongoClientConn, ErrMongoClientDisconn)
		}
		return nil, ErrMongoClientConn
	}

	return &mongoStore{
		client: cl,
		store:  cl.Database(opts.DBName),
	}, nil
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

	var stats clients.ConnPoolStats
	// var stats map[string]interface{}
	err = ms.client.Database(db).RunCommand(ctx, bson.D{{Key: "connPoolStats", Value: 1}}).Decode(&stats)
	if err != nil {
		l.Error("stats error", slog.String("error", err.Error()))
	}

	l.Debug("mongo client stats", slog.Any("stats", stats))
}

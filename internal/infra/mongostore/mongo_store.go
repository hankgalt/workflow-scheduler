package mongostore

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
)

const DEFAULT_MONGO_POOL_SIZE = 10

const ERR_DUPLICATE_RECORD = "mongostore: error duplicate record"

var ErrDuplicateRecord = errors.New(ERR_DUPLICATE_RECORD)

type MongoStore struct {
	client *mongo.Client
	store  *mongo.Database
}

func NewMongoStore(ctx context.Context, cfg infra.StoreConfig) (*MongoStore, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	builder := NewMongoConnectionBuilder(
		cfg.Protocol(),
		cfg.Host(),
	).WithUser(
		cfg.User(),
	).WithPassword(
		cfg.Pwd(),
	).WithDatabase(
		cfg.Name(),
	).WithConnectionParams(
		cfg.Params(),
	)

	clOpts := infra.DefaultClientOption()
	clOpts.DialTimeout = 5 * time.Minute
	clOpts.KeepAlive = 5 * time.Minute
	clOpts.KeepAliveTimeout = 2 * time.Minute

	opts := &MongoStoreOption{
		ClientOption: clOpts,
		DBName:       cfg.Name(),              // Database name is required for MongoDB operations
		PoolSize:     DEFAULT_MONGO_POOL_SIZE, // Default pool size, can be adjusted based on application needs
	}

	if opts.DBName == "" {
		return nil, ErrMissingDBName
	}

	dbConnStr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	mOpts := options.Client().ApplyURI(
		dbConnStr,
	).SetReadPreference(
		readpref.Nearest(),
	).SetAppName(
		opts.Caller,
	).SetMaxPoolSize(
		opts.PoolSize,
	)

	cl, err := mongo.Connect(ctx, mOpts)
	if err != nil {

		var we mongo.CommandError
		if errors.As(err, &we) {
			if we.Code == 10107 || we.Code == 13435 { // NotMaster / NotWritablePrimary codes
				l.Warn("MongoDB is not the primary", "error", we)
			}
		}

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

	return &MongoStore{
		client: cl,
		store:  cl.Database(opts.DBName),
	}, nil
}

func (ms *MongoStore) Store() *mongo.Database {
	return ms.store
}

func (ms *MongoStore) Close(ctx context.Context) error {
	if err := ms.client.Disconnect(ctx); err != nil && err != mongo.ErrClientDisconnected {
		return ErrMongoClientDisconn
	}
	return nil
}

func (ms *MongoStore) Stats(ctx context.Context, db string) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	var stats infra.ConnPoolStats
	err = ms.client.Database(db).RunCommand(ctx, bson.D{{Key: "connPoolStats", Value: 1}}).Decode(&stats)
	if err != nil {
		l.Error("stats error", slog.String("error", err.Error()))
	}

	l.Debug("mongo client stats", slog.Any("stats", stats))
}

func (ms *MongoStore) EnsureIndexes(
	ctx context.Context,
	collectionName string,
	indexes []mongo.IndexModel,
) error {
	collection := ms.store.Collection(collectionName)
	_, err := collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return err
	}
	return nil
}

func (ms *MongoStore) AddCollectionDoc(
	ctx context.Context,
	collectionName string,
	doc map[string]any,
) (string, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if collectionName == "" || doc == nil {
		return "", ErrMissingCollectionOrDoc
	}

	now := time.Now().UTC()
	doc["created_at"] = now
	doc["updated_at"] = now

	coll := ms.store.Collection(collectionName)
	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return "", ErrDuplicateRecord
		}

		var we mongo.CommandError
		if errors.As(err, &we) {
			if we.Code == 10107 || we.Code == 13435 { // NotMaster / NotWritablePrimary codes
				l.Error("error inserting document", "error", err.Error(), "collection", collectionName)
				return "", ErrMongoClientConn
			}
		}

		l.Error("error inserting document", "error", err.Error(), "collection", collectionName)
		return "", err
	}

	id, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		l.Error("error decoding inserted ID", "res", res)
		return "", ErrDecodeObjectId
	}
	return id.Hex(), nil
}

package infra

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type ShutdownFunc func(context.Context) error

var (
	defaultDialTimeout      = 5 * time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

type DBStore interface {
	Store() *mongo.Database
	EnsureIndexes(ctx context.Context, collectionName string, indexes []mongo.IndexModel) error
	Stats(ctx context.Context, db string)
	Close(ctx context.Context) error
}

type StoreConfig interface {
	Protocol() string
	Host() string
	User() string
	Pwd() string
	Params() string
	Name() string
}

type ConnPoolStats struct {
	Available      int64 `bson:"available"`
	Created        int64 `bson:"created"`
	TotalAvailable int64 `bson:"totalAvailable"`
	TotalCreated   int64 `bson:"totalCreated"`
	TotalInUse     int64 `bson:"totalInUse"`
	TotalLeased    int64 `bson:"totalLeased"`
	TotalRefreshed int64 `bson:"totalRefreshed"`
}

type ClientOption struct {
	Caller           string
	DialTimeout      time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
}

func DefaultClientOption() *ClientOption {
	return &ClientOption{
		DialTimeout:      defaultDialTimeout,
		KeepAlive:        defaultKeepAlive,
		KeepAliveTimeout: defaultKeepAliveTimeout,
	}
}

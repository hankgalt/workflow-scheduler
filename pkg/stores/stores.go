package stores

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type DBStore interface {
	Store() *mongo.Database
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

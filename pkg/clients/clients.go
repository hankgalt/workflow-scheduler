package clients

import (
	"time"
)

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	defaultDialTimeout      = 5 * time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

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

type ConnPoolStats struct {
	Available      int64 `bson:"available"`
	Created        int64 `bson:"created"`
	TotalAvailable int64 `bson:"totalAvailable"`
	TotalCreated   int64 `bson:"totalCreated"`
	TotalInUse     int64 `bson:"totalInUse"`
	TotalLeased    int64 `bson:"totalLeased"`
	TotalRefreshed int64 `bson:"totalRefreshed"`
}

type Filter struct {
	Field    string
	Value    string
	Operator string
	Type     string
}

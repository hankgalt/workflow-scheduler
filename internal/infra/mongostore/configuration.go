package mongostore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/hankgalt/workflow-scheduler/internal/infra"
)

const (
	ERR_REQUIRED_PARAMS      = "host & protocol are required"
	ERR_REQUIRED_CONN_PARAMS = "connection params are required"
)

var (
	ErrRequiredParams     = errors.New(ERR_REQUIRED_PARAMS)
	ErrRequiredConnParams = errors.New(ERR_REQUIRED_CONN_PARAMS)
)

type MongoDBConfig struct {
	protocol string
	host     string
	user     string
	pwd      string
	params   string
	name     string
}

func NewMongoDBConfig(protocol, host, user, pwd, params, name string) MongoDBConfig {
	return MongoDBConfig{
		protocol: protocol,
		host:     host,
		user:     user,
		pwd:      pwd,
		params:   params,
		name:     name,
	}
}

func (rc MongoDBConfig) Protocol() string { return rc.protocol }
func (rc MongoDBConfig) Host() string     { return rc.host }
func (rc MongoDBConfig) User() string     { return rc.user }
func (rc MongoDBConfig) Pwd() string      { return rc.pwd }
func (rc MongoDBConfig) Params() string   { return rc.params }
func (rc MongoDBConfig) Name() string     { return rc.name }

// ConnectionBuilder is an interface for building a MongoDB connection string.
type ConnectionBuilder interface {
	// Build returns a connection string based on the protocol, host, user, password, and
	// connection parameters. It returns an error if required parameters are missing or if the
	// protocol is "mongodb" and connection parameters are not provided.
	Build() (string, error)
	// WithUser sets the user for the connection string.
	WithUser(u string) ConnectionBuilder
	// WithPassword sets the password for the connection string.
	WithPassword(p string) ConnectionBuilder
	// WithConnectionParams sets the connection parameters for the connection string.
	WithConnectionParams(p string) ConnectionBuilder
}

// mongoConnectionBuilder is a ConnectionBuilder implementation for creating MongoDB connection strings.
type mongoConnectionBuilder struct {
	protocol string
	host     string
	user     string
	pwd      string
	params   string
}

// NewMongoConnectionBuilder creates a new mongoConnectionBuilder
// with the specified protocol and host & returns instance pointer.
func NewMongoConnectionBuilder(p, h string) mongoConnectionBuilder {
	return mongoConnectionBuilder{
		protocol: p,
		host:     h,
	}
}

func (b mongoConnectionBuilder) WithUser(u string) ConnectionBuilder {
	b.user = u
	return b
}

func (b mongoConnectionBuilder) WithPassword(p string) ConnectionBuilder {
	b.pwd = p
	return b
}

func (b mongoConnectionBuilder) WithConnectionParams(p string) ConnectionBuilder {
	b.params = p
	return b
}

// Build constructs the MongoDB connection string based on the provided parameters.
// It returns an error if required parameters are missing or if the protocol is "mongodb" and
// connection parameters are not provided.
// The connection string is formatted as "[protocol]://[user[:password]@]host[:port][/params]".
func (b mongoConnectionBuilder) Build() (string, error) {
	if b.protocol == "" || b.host == "" {
		return "", ErrRequiredParams
	}

	if b.protocol == "mongodb" && b.params == "" {
		return "", ErrRequiredConnParams
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s://", b.protocol))

	if b.user != "" {
		sb.WriteString(b.user)
		if b.pwd != "" {
			sb.WriteString(":" + b.pwd)
		}
		sb.WriteString("@")
	}

	sb.WriteString(b.host)

	if b.protocol == "mongodb" {
		sb.WriteString("/" + b.params)
	}

	return sb.String(), nil
}

// MongoClientOption holds the options for creating a new MongoDB client.
type MongoStoreOption struct {
	*infra.ClientOption
	DBName   string
	PoolSize uint64
}

func GetMongoConfig() infra.StoreConfig {
	dbProtocol := os.Getenv("MONGO_PROTOCOL")
	dbHost := os.Getenv("MONGO_HOSTNAME")
	dbUser := os.Getenv("MONGO_USERNAME")
	dbPwd := os.Getenv("MONGO_PASSWORD")
	dbParams := os.Getenv("MONGO_CONN_PARAMS")
	dbName := os.Getenv("MONGO_DBNAME")
	return NewMongoDBConfig(dbProtocol, dbHost, dbUser, dbPwd, dbParams, dbName)
}

func GetMongoStore(ctx context.Context, cfg infra.StoreConfig) (infra.DBStore, error) {
	builder := NewMongoConnectionBuilder(cfg.Protocol(), cfg.Host()).WithUser(cfg.User()).WithPassword(cfg.Pwd()).WithConnectionParams(cfg.Params())
	opts := &MongoStoreOption{
		ClientOption: infra.DefaultClientOption(),
		DBName:       cfg.Name(), // Database name is required for MongoDB operations
		PoolSize:     10,         // Default pool size, can be adjusted based on application needs
	}

	cl, err := NewMongoStore(ctx, opts, builder)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

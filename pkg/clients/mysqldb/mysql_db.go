package mysqldb

import (
	"fmt"
	"log"
	"os"

	"github.com/comfforts/errors"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	gormLogger "gorm.io/gorm/logger"
)

const (
	ERR_MISSING_REQUIRED = "error: missing required configuration"
	ERR_DB_CONNECTION    = "error: data store connection"
	ERR_DB_CLOSE         = "error: closing data store connection"

	ERR_MISSING_HOST = "error: missing host information"
	ERR_MISSING_USER = "error: missing DB user"
	ERR_MISSING_DB   = "error: missing DB name"
	ERR_MISSING_AUTH = "error: missing DB user auth"
)

var (
	ErrMissingRequired = errors.NewAppError(ERR_MISSING_REQUIRED)

	ErrMissingHost = errors.NewAppError(ERR_MISSING_HOST)
	ErrMissingUser = errors.NewAppError(ERR_MISSING_USER)
	ErrMissingDB   = errors.NewAppError(ERR_MISSING_DB)
	ErrMissingAuth = errors.NewAppError(ERR_MISSING_AUTH)
)

type DBClient interface {
	Close() error
}

type MYSQLDBClient struct {
	*gorm.DB
	logger *zap.Logger
}

type DBConfig struct {
	Host string
	Name string
	User string
	Auth string
}

func NewDBConfig(host, db, user, auth string) (*DBConfig, error) {
	if host == "" {
		host = os.Getenv("DB_HOST")
	}
	if db == "" {
		db = os.Getenv("DB_NAME")
	}
	if user == "" {
		user = os.Getenv("DB_USER")
	}
	if auth == "" {
		auth = os.Getenv("DB_PASSWORD")
	}

	if host == "" {
		return nil, ErrMissingHost
	}

	if user == "" {
		return nil, ErrMissingUser
	}

	if db == "" {
		return nil, ErrMissingDB
	}

	if auth == "" {
		return nil, ErrMissingAuth
	}

	return &DBConfig{
		Host: host,
		Name: db,
		User: user,
		Auth: auth,
	}, nil
}

func NewMYSQLClient(cfg *DBConfig, logger *zap.Logger) (*MYSQLDBClient, error) {
	connString := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=True&loc=Local", cfg.User, cfg.Auth, cfg.Host, cfg.Name)
	logger.Debug("initializing gorm mysql client", zap.String("db_host", cfg.Host), zap.String("db_name", cfg.Name), zap.String("db_user", cfg.User))

	db, err := gorm.Open(mysql.Open(connString),
		&gorm.Config{
			Logger: gormLogger.New(
				log.New(os.Stdout, "gorm", log.LstdFlags),
				gormLogger.Config{
					LogLevel: gormLogger.Error,
					Colorful: true,
				},
			),
		},
	)
	if err != nil {
		logger.Error(ERR_DB_CONNECTION, zap.Error(err))
		return nil, errors.WrapError(err, ERR_DB_CONNECTION)
	}

	return &MYSQLDBClient{
		DB: db,
	}, nil
}

func (cl *MYSQLDBClient) Close() error {
	db, err := cl.DB.DB()
	if err != nil {
		cl.logger.Error(ERR_DB_CONNECTION, zap.Error(err))
		return errors.WrapError(err, ERR_DB_CONNECTION)
	}
	err = db.Close()
	if err != nil {
		cl.logger.Error(ERR_DB_CLOSE, zap.Error(err))
		return errors.WrapError(err, ERR_DB_CLOSE)
	}
	return nil
}

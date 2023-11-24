package scheduler

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients"
)

const DEFAULT_SERVICE_PORT = "65051"
const DEFAULT_SERVICE_HOST = "127.0.0.1"

var (
	defaultDialTimeout      = 5 * time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

const SchedulerClientContextKey = clients.ContextKey("scheduler-client")

type ClientOption struct {
	DialTimeout      time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
	Caller           string
}

type Client interface {
	CreateRun(ctx context.Context, req *api.RunRequest, opts ...grpc.CallOption) (*api.RunResponse, error)
	UpdateRun(ctx context.Context, req *api.UpdateRunRequest, opts ...grpc.CallOption) (*api.RunResponse, error)
	GetRun(ctx context.Context, req *api.RunRequest, opts ...grpc.CallOption) (*api.RunResponse, error)
	DeleteRun(ctx context.Context, req *api.DeleteRunRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	AddEntity(ctx context.Context, req *api.AddEntityRequest, opts ...grpc.CallOption) (*api.AddEntityResponse, error)
	Close() error
}

func CalleeClientOptions(caller string) *ClientOption {
	opts := NewDefaultClientOption()
	opts.Caller = caller
	return opts
}

func NewDefaultClientOption() *ClientOption {
	return &ClientOption{
		DialTimeout:      defaultDialTimeout,
		KeepAlive:        defaultKeepAlive,
		KeepAliveTimeout: defaultKeepAliveTimeout,
	}
}

type schedulerClient struct {
	logger logger.AppLogger
	client api.SchedulerClient
	conn   *grpc.ClientConn
	opts   *ClientOption
}

func NewClient(l logger.AppLogger, clientOpts *ClientOption) (*schedulerClient, error) {
	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.SCHEDULER_CLIENT,
	})
	if err != nil {
		l.Error("error setting scheduler client TLS", zap.Error(err))
		return nil, err
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	servicePort := os.Getenv("SCHEDULER_SERVICE_PORT")
	if servicePort == "" {
		servicePort = DEFAULT_SERVICE_PORT
	}
	serviceHost := os.Getenv("SCHEDULER_SERVICE_HOST")
	if serviceHost == "" {
		serviceHost = DEFAULT_SERVICE_HOST
	}

	serviceAddr := fmt.Sprintf("%s:%s", serviceHost, servicePort)

	conn, err := grpc.Dial(serviceAddr, opts...)
	if err != nil {
		l.Error("client failed to connect", zap.Error(err))
		return nil, err
	}

	client := api.NewSchedulerClient(conn)

	return &schedulerClient{
		client: client,
		logger: l,
		conn:   conn,
		opts:   clientOpts,
	}, nil
}

func (bc *schedulerClient) CreateRun(ctx context.Context, req *api.RunRequest, opts ...grpc.CallOption) (*api.RunResponse, error) {
	ctx, cancel := bc.contextWithOptions(ctx, bc.opts)
	defer cancel()

	resp, err := bc.client.CreateRun(ctx, req)
	if err != nil {
		bc.logger.Error("error creating workflow run", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (bc *schedulerClient) UpdateRun(ctx context.Context, req *api.UpdateRunRequest, opts ...grpc.CallOption) (*api.RunResponse, error) {
	ctx, cancel := bc.contextWithOptions(ctx, bc.opts)
	defer cancel()

	resp, err := bc.client.UpdateRun(ctx, req)
	if err != nil {
		bc.logger.Error("error updating workflow run", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (bc *schedulerClient) GetRun(ctx context.Context, req *api.RunRequest, opts ...grpc.CallOption) (*api.RunResponse, error) {
	ctx, cancel := bc.contextWithOptions(ctx, bc.opts)
	defer cancel()

	resp, err := bc.client.GetRun(ctx, req)
	if err != nil {
		bc.logger.Error("error getting workflow run", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (bc *schedulerClient) DeleteRun(ctx context.Context, req *api.DeleteRunRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error) {
	ctx, cancel := bc.contextWithOptions(ctx, bc.opts)
	defer cancel()

	resp, err := bc.client.DeleteRun(ctx, req)
	if err != nil {
		bc.logger.Error("error deleting workflow run", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (bc *schedulerClient) AddEntity(ctx context.Context, req *api.AddEntityRequest, opts ...grpc.CallOption) (*api.AddEntityResponse, error) {
	ctx, cancel := bc.contextWithOptions(ctx, bc.opts)
	defer cancel()

	resp, err := bc.client.AddEntity(ctx, req)
	if err != nil {
		bc.logger.Error("error adding business entity", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (bc *schedulerClient) Close() error {
	if err := bc.conn.Close(); err != nil {
		bc.logger.Error("error closing scheduler client connection", zap.Error(err))
		return err
	}
	return nil
}

func (bc *schedulerClient) contextWithOptions(ctx context.Context, opts *ClientOption) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, bc.opts.DialTimeout)
	if bc.opts.Caller != "" {
		md := metadata.New(map[string]string{"service-client": bc.opts.Caller})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return ctx, cancel
}

package main

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	config "github.com/comfforts/comff-config"
	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/business/v1"
)

const SERVICE_PORT = 65052
const SERVICE_DOMAIN = "127.0.0.1"

func main() {

	// initialize app logger instance
	l := logger.GetSlogLogger()

	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{Target: config.CLIENT})
	if err != nil {
		l.Error("error setting client TLS", "error", err.Error())
		panic(err)
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	servicePort := fmt.Sprintf("%s:%d", SERVICE_DOMAIN, SERVICE_PORT)
	conn, err := grpc.Dial(servicePort, opts...)
	if err != nil {
		l.Error("client failed to connect", "error", err.Error())
		panic(err)
	}
	defer conn.Close()

	client := api.NewBusinessClient(conn)
	err = testEntityCRUD(client, l)
	if err != nil {
		l.Error("error: workflow CRUD", "error", err.Error())
		return
	}
}

func testEntityCRUD(client api.BusinessClient, l logger.Logger) error {
	// build id & entityId
	entity_num, entity_name := "5353427", "Zurn Concierge Nursing, Inc."

	// build Agent headers
	headers := []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "AGENT_TYPE", "ADDRESS"}

	// build first record
	values := []string{entity_name, entity_num, "", "Teri", "", "Zurn", "Chief Executive Officer", "23811 WASHINGTON AVE C-100 #184 MURRIETA CA  92562"}
	fields := map[string]string{}
	for i, k := range headers {
		fields[strings.ToLower(k)] = values[i]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// add agent
	resp, err := client.AddEntity(ctx, &api.AddEntityRequest{
		Fields: fields,
		Type:   api.EntityType_AGENT,
	})
	if err != nil {
		return err
	}

	// get agent
	gResp, err := client.GetEntity(ctx, &api.EntityRequest{
		Type: api.EntityType_AGENT,
		Id:   resp.GetAgent().Id,
	})
	if err != nil {
		return err
	}

	// delete agent
	_, err = client.DeleteEntity(ctx, &api.EntityRequest{
		Type: api.EntityType_AGENT,
		Id:   gResp.GetAgent().Id,
	})
	if err != nil {
		return err
	}

	return nil
}

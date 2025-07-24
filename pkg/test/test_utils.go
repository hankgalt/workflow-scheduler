package test

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/hankgalt/workflow-scheduler/pkg/clients"
	"github.com/hankgalt/workflow-scheduler/pkg/models/batch"
)

func GetTestLogger() *slog.Logger {
	logLevel := &slog.LevelVar{}

	logLevel.Set(slog.LevelInfo)
	if os.Getenv("INFRA") == "local" {
		logLevel.Set(slog.LevelDebug)
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	l := slog.New(handler)
	slog.SetDefault(l)
	return l
}

func BuildUserMigrationFilters() []clients.Filter {
	filters := []clients.Filter{}
	excludedUsers := GetExcludedUsers()
	filters = append(filters, clients.Filter{
		Field: "IsActive",
		Value: "true",
	})
	filters = append(filters, clients.Filter{
		Field:    "ExternalId",
		Value:    strings.Join(excludedUsers, ","),
		Operator: "$nin",
	})
	return filters
}

func BuildUserWalletFilters(filters []clients.Filter, isActive string, walletType, walletProvider string) []clients.Filter {
	if isActive == "true" || isActive == "false" {
		filters = append(filters, clients.Filter{
			Field: "IsWalletActive",
			Value: isActive,
		})
	}
	if walletType != "" {
		filters = append(filters, clients.Filter{
			Field: "WalletType",
			Value: walletType,
		})
	}
	if walletProvider != "" {
		filters = append(filters, clients.Filter{
			Field: "WalletProvider",
			Value: walletProvider,
		})
	}
	return filters
}

func BuildItemMigrationFilters(itemType string) []clients.Filter {
	filters := []clients.Filter{}
	excludedUsers := GetExcludedUsers()
	howTokenized := "OnChain"
	filters = append(filters, clients.Filter{
		Field: "Core.ItemType",
		Value: itemType,
	})
	filters = append(filters, clients.Filter{
		Field: "Core.HowTokenized",
		Value: howTokenized,
	})
	filters = append(filters, clients.Filter{
		Field:    "Core.ItemName",
		Value:    strings.Join([]string{"Placeholder"}, ","),
		Operator: "$nin",
	})
	filters = append(filters, clients.Filter{
		Field:    "Core.ExternalId",
		Value:    strings.Join([]string{"Placeholder"}, ","),
		Operator: "$nin",
	})
	filters = append(filters, clients.Filter{
		Field:    "Core.UserId",
		Value:    strings.Join(excludedUsers, ","),
		Operator: "$nin",
	})
	return filters
}

func GetExcludedUsers() []string {
	excludedUsers := []string{}
	userId1 := os.Getenv("DEV_MFG_USER_ID")
	if userId1 != "" {
		excludedUsers = append(excludedUsers, userId1)
	}
	userId2 := os.Getenv("STG_MFG_USER_ID")
	if userId2 != "" {
		excludedUsers = append(excludedUsers, userId2)
	}
	userId3 := os.Getenv("PROD_MFG_USER_ID")
	if userId3 != "" {
		excludedUsers = append(excludedUsers, userId3)
	}
	userId4 := os.Getenv("STG_OFF_USER_ID")
	if userId4 != "" {
		excludedUsers = append(excludedUsers, userId4)
	}
	userId5 := os.Getenv("PROD_OFF_USER_ID")
	if userId5 != "" {
		excludedUsers = append(excludedUsers, userId5)
	}
	return excludedUsers
}

func GetTestConfig() testConfig {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "data"
	}
	bucket := os.Getenv("BUCKET")
	credsPath := os.Getenv("CREDS_PATH")

	return testConfig{
		dir:       dataDir,
		bucket:    bucket,
		credsPath: credsPath,
	}
}

func ProcessCSVRecordStream(t *testing.T, ctx context.Context, recStream <-chan batch.Result) (int, int) {
	var processedCount, errorCount int

	for {
		select {
		case <-ctx.Done():
			t.Log("Context cancelled, stopping processing")
			return processedCount, errorCount
		case rec, ok := <-recStream:
			if !ok {
				t.Log("Record stream closed")
				return processedCount, errorCount
			}
			if rec.Error != "" {
				t.Logf("Error processing record: %s", rec.Error)
				errorCount++
			} else if rec.Record != nil {
				t.Logf("Processed count: %d, record: %v", processedCount+1, rec.Record)
				processedCount++
			}
		}
	}
}

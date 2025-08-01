package test

import (
	"os"
)

const TEST_DIR = "data"
const DATA_PATH string = "scheduler"
const MISSING_FILE_NAME string = "geo.csv"
const LIVE_FILE_NAME string = "Agents-sm.csv"

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

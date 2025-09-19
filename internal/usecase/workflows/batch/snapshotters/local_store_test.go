package snapshotters_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
	"github.com/stretchr/testify/require"
)

func TestGetLastOffset(t *testing.T) {
	l := logger.GetSlogLogger()

	fPath, err := envutils.BuildFilePath()
	require.NoError(t, err)
	l.Debug("file path", "path", fPath)

	// get current dir path
	dir, err := os.Getwd()
	require.NoError(t, err)
	l.Debug("current dir", "dir", dir)

	// get current dir path
	hDir, err := os.UserHomeDir()
	require.NoError(t, err)
	l.Debug("home dir", "home-dir", hDir)

	dataDir := "../../../../../cmd/workers/batch/data/scheduler"
	files, err := os.ReadDir(dataDir)
	require.NoError(t, err)

	var n uint64 = 0
	var fileName string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "local-csv-mongo-local-data-scheduler-Agents.csv") {
			tokens := strings.Split(file.Name(), "-")

			offset := strings.Replace(tokens[len(tokens)-1], ".json", "", 1)

			num, err := strconv.ParseUint(offset, 0, 64)
			require.NoError(t, err)

			if num > n {
				n = num
				fileName = file.Name()
			}

			fmt.Printf("file name: %s, offset: %d\n", file.Name(), num)
		}
	}

	l.Info("Latest file", "file-name", fileName, "offset", n)

	filePath := fmt.Sprintf("%s/%s", dataDir, fileName)

	fileBytes, err := os.ReadFile(filePath)
	require.NoError(t, err)

	var batchResult domain.BatchProcessingResult
	err = json.Unmarshal(fileBytes, &batchResult)
	require.NoError(t, err)

	l.Info("Batch processing result", "next-offset", batchResult.Offsets[len(batchResult.Offsets)-1])
}

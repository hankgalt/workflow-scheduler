package sources

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
	hp "github.com/hankgalt/workflow-scheduler/pkg/utils/heap"
)

const LocalJSONSource = "local-json-source"

const ERR_LOCAL_JSON_DIR_OPEN = "local json: open directory"
const ERR_LOCAL_JSON_FILE_OPEN = "local json: open file"
const ERR_LOCAL_JSON_FILE_PATH = "local json: path is required"
const ERR_LOCAL_JSON_SIZE_INVALID = "local json: size must be greater than 0"

var ErrLocalJSONDirOpen = errors.New(ERR_LOCAL_JSON_DIR_OPEN)
var ErrLocalJSONFileOpen = errors.New(ERR_LOCAL_JSON_FILE_OPEN)
var ErrLocalJSONPathRequired = errors.New(ERR_LOCAL_JSON_FILE_PATH)
var ErrLocalJSONSizeInvalid = errors.New(ERR_LOCAL_JSON_SIZE_INVALID)

type localJSONSource struct {
	path    string
	fileKey string
}

// Name of the source.
func (s *localJSONSource) Name() string { return LocalJSONSource }

// Close closes the local JSON source.
func (s *localJSONSource) Close(ctx context.Context) error {
	// No resources to close for local JSON source
	return nil
}

// Next reads the next batch of JSON objects from the local file.
// It reads from the file at the specified offset and returns a batch of JSON objects.
func (s *localJSONSource) Next(ctx context.Context, offset string, size uint) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalJSONSizeInvalid
	}

	bp := &domain.BatchProcess{
		Records:     nil,
		NextOffset:  offset,
		StartOffset: offset,
		Done:        false,
	}

	entries, err := os.ReadDir(s.path)
	if err != nil {
		l.Error("error reading local json directory", "path", s.path, "error", err.Error())
		return nil, ErrLocalJSONDirOpen
	}

	// var smallestFileOffset int64
	// var fileName strin
	maxHeap := &hp.MaxHeap[int64]{}
	fileMap := map[int64]string{}
	currFileOffset := int64(0)
	currOffsetFileName := ""

	errFileMap := map[string]string{}

	heap.Init(maxHeap)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), s.fileKey) || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		toks := strings.Split(entry.Name(), "-")
		if len(toks) > 1 {
			fileOffset := strings.Split(toks[len(toks)-1], ".")[0]

			fileOffsetInt64, err := utils.ParseInt64(fileOffset)
			if err != nil {
				l.Error("error parsing file offset", "file", entry.Name(), "error", err.Error())
				continue
			}

			if toks[len(toks)-2] != "error" {
				if fileOffsetInt64 > currFileOffset {
					currFileOffset = fileOffsetInt64
					currOffsetFileName = entry.Name()
				}

				heap.Push(maxHeap, fileOffsetInt64)
				fileMap[fileOffsetInt64] = entry.Name()

				if maxHeap.Len() > 3 {
					popped := heap.Pop(maxHeap)
					poppedInt64 := popped.(int64)
					delete(fileMap, poppedInt64)
				}
			} else {
				errFileMap[fileOffset] = entry.Name()
			}
		}
	}

	out := make([]int64, maxHeap.Len())
	for i := range out {
		outInt64 := heap.Pop(maxHeap).(int64)
		out[i] = outInt64
	}
	slices.Sort(out)

	l.Debug("json file directory", "path", s.path)
	l.Debug("files", "smallest-file", fileMap[out[0]], "current-file", currOffsetFileName)
	errFilesToProcess := []string{}
	for k, v := range errFileMap {
		fileOffsetInt64, err := utils.ParseInt64(k)
		if err != nil {
			l.Error("error parsing error file offset", "file", v, "error", err.Error())
			continue
		}
		if fileOffsetInt64 < currFileOffset {
			// l.Debug("errors already resolved with restarted processing, will delete file", "error-file", v, "current-file-offset", currFileOffset)
			continue
		}
		l.Debug("errors still present, will restart processing from this offset", "error-file", v, "err-offset", fileOffsetInt64, "current-offset", currFileOffset)
		errFilesToProcess = append(errFilesToProcess, v)
	}

	// Open the JSON file
	filePath := filepath.Join(s.path, fileMap[out[0]])
	file, err := os.Open(filePath)
	if err != nil {
		l.Error("error opening local json file", "path", filePath, "error", err.Error())
		return nil, err
	}
	defer file.Close() // Ensure the file is closed

	var data domain.BatchProcessingResult
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		l.Error("error decoding local json file", "path", filePath, "error", err.Error())
		return nil, err
	}

	nextOffset := data.Offsets[len(data.Offsets)-1]
	bp.Records = []*domain.BatchRecord{
		{
			Data:  data,
			Start: offset,
			End:   nextOffset,
		},
	}
	bp.NextOffset = nextOffset

	l.Debug("batch result json file to process", "file", fileMap[out[0]])
	l.Debug("file details", "num-records", data.NumRecords, "num-batches", data.NumBatches, "next-offset", nextOffset, "next-file-offset", out[1])
	if data.Error != "" {
		l.Debug("file error", "error", data.Error, "file", fileMap[out[0]])
	}
	if len(errFilesToProcess) > 0 {
		l.Debug("error files", "err-files", errFilesToProcess)
	}

	return bp, nil
}

// Local JSON source config.
type LocalJSONConfig struct {
	Path    string
	FileKey string
}

// Name of the source.
func (c *LocalJSONConfig) Name() string { return LocalJSONSource }

// BuildSource builds a local JSON source from the config.
func (c *LocalJSONConfig) BuildSource(ctx context.Context) (domain.Source[any], error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if c.Path == "" {
		l.Error("local json source: path is required")
		return nil, ErrLocalJSONPathRequired
	}

	return &localJSONSource{
		path:    c.Path,
		fileKey: c.FileKey,
	}, nil
}

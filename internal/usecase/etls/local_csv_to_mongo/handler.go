package localcsvtomongo

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/repo/vypar"
	localcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/local_csv"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

type LocalCSVToMongoHandler struct {
	dataPoint    batch.DataPoint
	csvHandler   *localcsv.LocalCSVFileHandler
	mongoHandler vypar.VyparRepo
}

func NewLocalCSVToMongoHandler(ctx context.Context, cfg localcsv.LocalCSVFileHandlerConfig, sCfg infra.StoreConfig) (*LocalCSVToMongoHandler, error) {
	if cfg.Name() == "" {
		return nil, localcsv.ErrMissingFileName
	}

	csvHandler, err := localcsv.NewLocalCSVFileHandler(cfg)
	if err != nil {
		return nil, err
	}

	mongoStore, err := mongostore.NewMongoStore(ctx, sCfg)
	if err != nil {
		return nil, err
	}
	vr, err := vypar.NewVyparRepo(ctx, mongoStore)
	if err != nil {
		return nil, err
	}

	return &LocalCSVToMongoHandler{
		dataPoint:    batch.DataPoint{Name: cfg.Name(), Path: cfg.Path()},
		csvHandler:   csvHandler,
		mongoHandler: vr,
	}, nil
}

func (h *LocalCSVToMongoHandler) Headers() []string {
	return h.csvHandler.Headers()
}

func (h *LocalCSVToMongoHandler) ReadData(ctx context.Context, offset, limit uint64) (any, uint64, bool, error) {
	return h.csvHandler.ReadData(ctx, offset, limit)
}

func (h *LocalCSVToMongoHandler) HandleData(ctx context.Context, start uint64, data any, headers []string) (<-chan batch.Result, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("LocalCSVToMongoHandler:HandleData - error getting logger from context: %w", err)
	}

	chnk, ok := data.([]byte)
	if !ok {
		return nil, localcsv.ErrInvalidDataType
	}

	resStream := make(chan batch.Result)
	go func() {
		defer close(resStream)

		buffer := bytes.NewBuffer(chnk)
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

		currOffset := csvReader.InputOffset()
		lastOffset := currOffset
		for {
			record, err := csvReader.Read()
			if err != nil {
				if err == io.EOF {
					l.Debug("LocalCSVToMongoHandler:HandleData - reached EOF, ending go routine",
						"lastRecordStart", lastOffset,
						"lastRecordEnd", currOffset,
					)
					return
				} else {
					cleanedStr := strutils.CleanRecord(string(chnk[currOffset:csvReader.InputOffset()]))
					record, err = strutils.ReadSingleRecord(cleanedStr)
					if err != nil {
						resStream <- batch.Result{
							Start: start + uint64(lastOffset),
							End:   start + uint64(currOffset),
							Error: err.Error(),
						}
						continue
					}
				}
			}

			lastOffset, currOffset = currOffset, csvReader.InputOffset()
			cleanedRec := strutils.CleanAlphaNumericsArr(record, []rune{'.', '-', '_', '#', '&', '@'})

			fields := map[string]string{}
			for i, field := range headers {
				fields[strings.ToLower(field)] = cleanedRec[i]
			}

			mongoModel := stores.MapAgentFieldsToMongoModel(fields)
			agID, err := h.mongoHandler.AddAgent(ctx, mongoModel)
			if err != nil {
				resStream <- batch.Result{
					Start: start + uint64(lastOffset),
					End:   start + uint64(currOffset),
					Error: err.Error(),
				}
				continue
			}

			resStream <- batch.Result{
				Start:  start + uint64(lastOffset),
				End:    start + uint64(currOffset),
				Record: map[string]string{"recordId": agID},
			}
		}
	}()
	return resStream, nil
}

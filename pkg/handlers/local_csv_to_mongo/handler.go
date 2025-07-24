package localcsvtomongo

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	localcsv "github.com/hankgalt/workflow-scheduler/pkg/handlers/local_csv"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/models/batch"
	"github.com/hankgalt/workflow-scheduler/pkg/repos/vypar"
	"github.com/hankgalt/workflow-scheduler/pkg/stores"
	"github.com/hankgalt/workflow-scheduler/pkg/stores/mongostore"
	"github.com/hankgalt/workflow-scheduler/pkg/utils"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

type LocalCSVToMongoHandler struct {
	dataPoint    batch.DataPoint
	csvHandler   *localcsv.LocalCSVFileHandler
	mongoHandler vypar.VyparRepo
}

func NewLocalCSVToMongoHandler(ctx context.Context, cfg localcsv.LocalCSVFileHandlerConfig, sCfg stores.StoreConfig) (*LocalCSVToMongoHandler, error) {
	if cfg.Name() == "" {
		return nil, localcsv.ErrMissingFileName
	}

	csvHandler, err := localcsv.NewLocalCSVFileHandler(cfg)
	if err != nil {
		return nil, err
	}

	mongoStore, err := mongostore.GetMongoStore(ctx, sCfg)
	if err != nil {
		return nil, err
	}
	vr, err := vypar.NewVyparRepo(mongoStore)
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
	chnk, ok := data.([]byte)
	if !ok {
		return nil, errors.New("invalid data type, expected []byte")
	}

	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get logger from context: %w", err)
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
					l.Debug("HandleData - reached EOF, ending go routine",
						"lastRecordStart", lastOffset,
						"lastRecordEnd", currOffset,
					)
					return
				} else {
					cleanedStr := utils.CleanRecord(string(chnk[currOffset:csvReader.InputOffset()]))
					record, err = utils.ReadSingleRecord(cleanedStr)
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
			cleanedRec := utils.CleanAlphaNumericsArr(record, []rune{'.', '-', '_', '#', '&', '@'})

			fields := map[string]string{}
			for i, field := range headers {
				fields[strings.ToLower(field)] = cleanedRec[i]
			}

			mongoModel := models.MapAgentFieldsToMongoModel(fields)
			if agID, err := h.mongoHandler.AddAgent(ctx, mongoModel); err != nil {
				resStream <- batch.Result{
					Start: start + uint64(lastOffset),
					End:   start + uint64(currOffset),
					Error: err.Error(),
				}
				continue
			} else {
				fields["recordId"] = agID
			}

			resStream <- batch.Result{
				Start:  start + uint64(lastOffset),
				End:    start + uint64(currOffset),
				Record: fields,
			}
		}
	}()
	return resStream, nil
}

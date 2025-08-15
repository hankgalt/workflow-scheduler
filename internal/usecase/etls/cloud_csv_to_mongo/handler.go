package cloudcsvtomongo

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/etls"
	cloudcsv "github.com/hankgalt/workflow-scheduler/internal/usecase/etls/cloud_csv"
	strutils "github.com/hankgalt/workflow-scheduler/pkg/utils/string"
)

type CloudCSVToMongoHandler struct {
	dataPoint    batch.CSVDataPoint
	csvHandler   batch.CSVDataProcessorWithClose
	mongoHandler mongostore.MongoStore
}

func NewCloudCSVToMongoHandler(
	ctx context.Context,
	cfg cloudcsv.CloudCSVFileHandlerConfig,
	sCfg infra.StoreConfig,
	coll string,
) (*CloudCSVToMongoHandler, error) {
	if cfg.Name() == "" {
		return nil, cloudcsv.ErrMissingFileName
	}

	if cfg.Bucket() == "" {
		return nil, cloudcsv.ErrMissingBucketName
	}

	csvHandler, err := cloudcsv.NewCloudCSVFileHandler(cfg)
	if err != nil {
		return nil, err
	}

	mongoStore, err := mongostore.NewMongoStore(ctx, sCfg)
	if err != nil {
		return nil, err
	}
	return &CloudCSVToMongoHandler{
		dataPoint: batch.CSVDataPoint{
			Name:       cfg.Name(),
			Path:       cfg.Path(),
			Bucket:     cfg.Bucket(),
			Collection: coll,
		},
		csvHandler:   csvHandler,
		mongoHandler: mongoStore,
	}, nil
}

func (h *CloudCSVToMongoHandler) Headers() []string {
	return h.csvHandler.Headers()
}

func (h *CloudCSVToMongoHandler) ReadData(
	ctx context.Context,
	offset, limit uint64,
) (any, uint64, bool, error) {
	return h.csvHandler.ReadData(ctx, offset, limit)
}

func (h *CloudCSVToMongoHandler) HandleData(
	ctx context.Context,
	start uint64,
	data any,
	transFunc batch.TransformerFunc,
) (<-chan batch.Result, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("CloudCSVToMongoHandler:HandleData - error getting logger from context: %w", err)
	}

	chnk, ok := data.([]byte)
	if !ok {
		return nil, etls.ErrInvalidDataType
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
					l.Debug("CloudCSVToMongoHandler:HandleData - reached EOF, ending go routine",
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

			fields := transFunc(cleanedRec)

			resID, err := h.mongoHandler.AddCollectionDoc(ctx, h.dataPoint.Collection, fields)
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
				Record: map[string]string{"mongoId": resID},
			}
		}

	}()

	return resStream, nil
}

package models

import (
	"fmt"
	"strconv"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
)

type HeadersInfo struct {
	Headers []string
	Offset  int64
}

type HeadersInfoState struct {
	Headers []string
	Offset  string
}

type SuccessResult struct {
	Start, End int64
	Id         string
}

type SuccessResultState struct {
	Start, End string
	Id         string
}

type ErrorResult struct {
	Start, End int64
	Error      string
}

type ErrorResultState struct {
	Start, End string
	Error      string
}

type CSVBatchResult struct {
	Start, End int64
	BatchIndex int
	Results    []*SuccessResult
	Errors     []*ErrorResult
}

type CSVBatchResultState struct {
	Start, End string
	BatchIndex string
	Results    []*SuccessResultState
	Errors     []*ErrorResultState
}

type CSVInfo struct {
	FileName          string
	RequestedBy       string
	HostID            string
	RunId             string
	WorkflowId        string
	ProcessRunId      string
	ProcessWorkflowId string
	FileSize          int64
	Headers           *HeadersInfo
	OffSets           []int64
	Results           map[int64]*CSVBatchResult
	Type              EntityType
}

type CSVInfoState struct {
	FileName          string
	RequestedBy       string
	HostID            string
	RunId             string
	WorkflowId        string
	ProcessRunId      string
	ProcessWorkflowId string
	FileSize          string
	Headers           *HeadersInfoState
	OffSets           []string
	Results           map[string]*CSVBatchResultState
	Type              EntityType
}

type ReadRecordsParams struct {
	FileName    string
	RequestedBy string
	HostID      string
	RunId       string
	WorkflowId  string
	Type        EntityType
	Headers     *HeadersInfo
	BatchIndex  int
	Start       int64
	End         int64
	Results     []*SuccessResult
	Errors      []*ErrorResult
}

type CSVResultSignal struct {
	Id    string
	Start int64
	Size  int64
}

type CSVErrSignal struct {
	Error string
	Start int64
	Size  int64
}

func IntToString(i int) string {
	return strconv.Itoa(i)
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func MapInt64SliceToStringSlice(int64Slice []int64) []string {
	var stringSlice []string
	for _, v := range int64Slice {
		stringSlice = append(stringSlice, Int64ToString(v))
	}
	return stringSlice
}

func MapHeadersInfoToState(headersInfo *HeadersInfo) *HeadersInfoState {
	return &HeadersInfoState{
		Headers: headersInfo.Headers,
		Offset:  Int64ToString(headersInfo.Offset),
	}
}

func MapSuccessResultsToState(res []*SuccessResult) []*SuccessResultState {
	var stRes []*SuccessResultState
	for _, v := range res {
		stRes = append(stRes, MapSuccessResultToState(v))
	}
	return stRes
}

func MapSuccessResultToState(successResult *SuccessResult) *SuccessResultState {
	return &SuccessResultState{
		Start: Int64ToString(successResult.Start),
		End:   Int64ToString(successResult.End),
		Id:    successResult.Id,
	}
}

func MapErrorResultsToState(res []*ErrorResult) []*ErrorResultState {
	var stRes []*ErrorResultState
	for _, v := range res {
		stRes = append(stRes, MapErrorResultToState(v))
	}
	return stRes
}

func MapErrorResultToState(errorResult *ErrorResult) *ErrorResultState {
	return &ErrorResultState{
		Start: Int64ToString(errorResult.Start),
		End:   Int64ToString(errorResult.End),
		Error: errorResult.Error,
	}
}

func MapCSVBatchResultMapToState(csvBatchResultMap map[int64]*CSVBatchResult) map[string]*CSVBatchResultState {
	var csvBatchResultStateMap = make(map[string]*CSVBatchResultState)
	for k, v := range csvBatchResultMap {
		csvBatchResultStateMap[Int64ToString(k)] = MapCSVBatchResultToState(v)
	}
	return csvBatchResultStateMap
}

func MapCSVBatchResultToState(csvBatchResult *CSVBatchResult) *CSVBatchResultState {
	return &CSVBatchResultState{
		Start:      Int64ToString(csvBatchResult.Start),
		End:        Int64ToString(csvBatchResult.End),
		BatchIndex: IntToString(csvBatchResult.BatchIndex),
		Results:    MapSuccessResultsToState(csvBatchResult.Results),
		Errors:     MapErrorResultsToState(csvBatchResult.Errors),
	}
}

func MapCSVInfoToState(csvInfo *CSVInfo) CSVInfoState {
	return CSVInfoState{
		FileName:          csvInfo.FileName,
		RequestedBy:       csvInfo.RequestedBy,
		HostID:            csvInfo.HostID,
		RunId:             csvInfo.RunId,
		WorkflowId:        csvInfo.WorkflowId,
		ProcessRunId:      csvInfo.ProcessRunId,
		ProcessWorkflowId: csvInfo.ProcessWorkflowId,
		FileSize:          Int64ToString(csvInfo.FileSize),
		Headers:           MapHeadersInfoToState(csvInfo.Headers),
		OffSets:           MapInt64SliceToStringSlice(csvInfo.OffSets),
		Results:           MapCSVBatchResultMapToState(csvInfo.Results),
		Type:              csvInfo.Type,
	}
}

func MapFileWorkflowStateToProto(state map[string]interface{}) *api.FileWorkflowState {
	stateProto := &api.FileWorkflowState{}

	for k, v := range state {
		switch v := v.(type) {
		case map[string]interface{}:
			switch k {
			case "Headers":
				stateProto.Headers = &api.HeaderState{}
				for key, value := range v {
					switch key {
					case "Headers":
						for _, val := range value.([]interface{}) {
							stateProto.Headers.Headers = append(stateProto.Headers.Headers, val.(string))
						}
					case "Offset":
						stateProto.Headers.Offset = value.(string)
					default:
						fmt.Printf("default: - %s: %v\n", key, value)
					}
				}
			case "Results":
				stateProto.Batches = []*api.BatchState{}
				for rkey, rVal := range v {
					switch rVal := rVal.(type) {
					case map[string]interface{}:
						batch := &api.BatchState{}
						for key, value := range rVal {
							switch key {
							case "BatchIndex":
								batch.BatchIndex = value.(string)
							case "Start":
								batch.Start = value.(string)
							case "End":
								batch.End = value.(string)
							case "Results":
								if value != nil {
									batch.Results = []*api.SuccessResultState{}
									for _, val := range value.([]interface{}) {
										sResult := &api.SuccessResultState{}
										for k, v := range val.(map[string]interface{}) {
											switch k {
											case "Start":
												sResult.Start = v.(string)
											case "End":
												sResult.End = v.(string)
											case "Id":
												sResult.ResultId = v.(string)
											default:
												fmt.Printf("default: - %s: %v\n", k, v)
											}
										}
										batch.Results = append(batch.Results, sResult)
									}
								}
							case "Errors":
								batch.Errors = []*api.ErrorResultState{}
								if value != nil {
									batch.Errors = []*api.ErrorResultState{}
									for _, val := range value.([]interface{}) {
										result := &api.ErrorResultState{}
										for k, v := range val.(map[string]interface{}) {
											switch k {
											case "Start":
												result.Start = v.(string)
											case "End":
												result.End = v.(string)
											case "Error":
												result.Error = v.(string)
											default:
												fmt.Printf("default: - %s: %v\n", k, v)
											}
										}
										batch.Errors = append(batch.Errors, result)
									}
								}
							default:
								fmt.Printf("default: - %s: %v\n", key, value)
							}
						}
						stateProto.Batches = append(stateProto.Batches, batch)
					default:
						fmt.Printf("default: - %s: %v\n", rkey, rVal)
					}
				}
			default:
				fmt.Printf("default: - %s: %v\n", k, v)
			}
		case string:
			switch k {
			case "FileName":
				stateProto.FileName = v
			case "RequestedBy":
				stateProto.RequestedBy = v
			case "HostID":
				stateProto.HostId = v
			case "RunId":
				stateProto.RunId = v
			case "WorkflowId":
				stateProto.WorkflowId = v
			case "ProcessRunId":
				stateProto.ProcessRunId = v
			case "ProcessWorkflowId":
				stateProto.ProcessWorkflowId = v
			case "FileSize":
				stateProto.FileSize = v
			case "Type":
				stateProto.Type = v
			default:
				fmt.Printf("default: - %s: %v\n", k, v)
			}
		case []interface{}:
			switch k {
			case "OffSets":
				for _, val := range v {
					stateProto.Offsets = append(stateProto.Offsets, val.(string))
				}
			default:
				fmt.Printf("default: - %s: %v\n", k, v)
			}
		default:
			fmt.Printf("default: - %s: %v\n", k, v)
		}
	}

	return stateProto
}

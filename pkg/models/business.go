package models

import "strconv"

type HeadersInfo struct {
	Headers []string
	Offset  int64
}

type HeadersInfoState struct {
	Headers []string
	Offset  string
}

type CSVBatchResult struct {
	Start, End  int64
	BatchIndex  int
	Count       int
	ResultCount int
	ErrCount    int
}

type CSVBatchResultState struct {
	Start, End  string
	BatchIndex  string
	Count       string
	ResultCount string
	ErrCount    string
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

func MapCSVBatchResultMapToState(csvBatchResultMap map[int64]*CSVBatchResult) map[string]*CSVBatchResultState {
	var csvBatchResultStateMap = make(map[string]*CSVBatchResultState)
	for k, v := range csvBatchResultMap {
		csvBatchResultStateMap[Int64ToString(k)] = MapCSVBatchResultToState(v)
	}
	return csvBatchResultStateMap
}

func MapCSVBatchResultToState(csvBatchResult *CSVBatchResult) *CSVBatchResultState {
	return &CSVBatchResultState{
		Start:       Int64ToString(csvBatchResult.Start),
		End:         Int64ToString(csvBatchResult.End),
		BatchIndex:  IntToString(csvBatchResult.BatchIndex),
		Count:       IntToString(csvBatchResult.Count),
		ResultCount: IntToString(csvBatchResult.ResultCount),
		ErrCount:    IntToString(csvBatchResult.ErrCount),
	}
}

func IntToString(i int) string {
	return strconv.Itoa(i)
}

type WorkflowState struct {
	state *CSVInfo
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
	Count       int
	ResultCount int
	ErrCount    int
}

type CSVResultSignal struct {
	Record []string
	Start  int64
	Size   int64
}

type CSVErrSignal struct {
	Error string
	Start int64
	Size  int64
}

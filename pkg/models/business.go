package models

import api "github.com/hankgalt/workflow-scheduler/api/v1"

type CSVBatchResult struct {
	Start, End  int64
	BatchIndex  int
	Count       int
	ResultCount int
	ErrCount    int
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
	Type              api.EntityType
}

type HeadersInfo struct {
	Headers []string
	Offset  int64
}

type ReadRecordsParams struct {
	FileName    string
	RequestedBy string
	HostID      string
	RunId       string
	WorkflowId  string
	Type        api.EntityType
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

package models

type FileSignal struct {
	RequestedBy string
	FilePath    string
	Done        bool
}

type FileSignalParams struct {
	RequestedBy string
	FilePath    string
	Type        EntityType
}

type WorkflowQueryParams struct {
	RunId      string
	WorkflowId string
}

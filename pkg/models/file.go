package models

type FileSignal struct {
	RequestedBy string
	FilePath    string
	Done        bool
}

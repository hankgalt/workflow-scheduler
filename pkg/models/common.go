package models

type JSONMapper = map[string]interface{}
type CSVRecord = map[string]string

type BatchInfo struct {
	BatchCount     int
	ProcessedCount int
	ErrCount       int
}

package models

type JSONMapper = map[string]interface{}
type CSVRecord = map[string]string

type BatchInfo struct {
	BatchCount     int
	ProcessedCount int
	ErrCount       int
}

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const LoggerContextKey = ContextKey("logger")
const HeadersContextKey = ContextKey("csv-headers")

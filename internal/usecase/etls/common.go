package etls

import "errors"

const (
	ERR_MISSING_FILE_NAME = "missing file name"
	ERR_INVALID_DATA_TYPE = "invalid data type, expected []byte"
)

var (
	ErrMissingFileName = errors.New(ERR_MISSING_FILE_NAME)
	ErrInvalidDataType = errors.New(ERR_INVALID_DATA_TYPE)
)

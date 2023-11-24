package file

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
)

func CreateJSONFile(dir, name string) (string, error) {
	fPath := fmt.Sprintf("%s/%s", dir, name)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return "", err
	}

	items := CreateStoreJSONList()

	f, err := os.Create(fPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(items)
	if err != nil {
		return "", err
	}
	return fPath, nil
}

func CreateStoreJSONList() []models.JSONMapper {
	items := []models.JSONMapper{
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Plaza Hollywood",
			"country":   "CN",
			"longitude": 114.20169067382812,
			"latitude":  22.340700149536133,
			"storeId":   1,
		},
		{
			"city":      "Hong Kong",
			"org":       "starbucks",
			"name":      "Exchange Square",
			"country":   "CN",
			"longitude": 114.15818786621094,
			"latitude":  22.283939361572266,
			"storeId":   6,
		},
		{
			"city":      "Kowloon",
			"org":       "starbucks",
			"name":      "Telford Plaza",
			"country":   "CN",
			"longitude": 114.21343994140625,
			"latitude":  22.3228702545166,
			"storeId":   8,
		},
	}
	return items
}

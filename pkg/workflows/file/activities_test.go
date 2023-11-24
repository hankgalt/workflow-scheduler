package file_test

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestFilePath(t *testing.T) {
	filePath := "/data/test.json"
	fmt.Println("filePath - ", filePath)

	dir := filepath.Dir(filePath)
	fmt.Println("dir - ", dir)

	base := filepath.Base(filePath)
	fmt.Println("base - ", base)

	ext := filepath.Ext(filePath)
	fmt.Println("ext - ", ext)

	fmt.Println("filePath - ", filePath)
}

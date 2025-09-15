package string

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"unicode"

	"github.com/comfforts/logger"
)

// MapTokens maps each token in the input slice to its corresponding value in the mapping.
// If a token does not exist in the mapping, it retains its original value.
func MapTokens(tokens []string, mapping map[string]string) []string {
	mappedTokens := make([]string, len(tokens))
	for i, token := range tokens {
		if mappedToken, exists := mapping[token]; exists {
			mappedTokens[i] = mappedToken
		} else {
			mappedTokens[i] = token // Keep original if no mapping found
		}
	}
	return mappedTokens
}

// LowerFirst converts the first character of the string to lowercase.
// If the string is empty, it returns an empty string.
func LowerFirst(s string) string {
	if len(s) == 0 {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToLower(r[0])

	return string(r)
}

// CleanAlphaNumerics removes non-alphanumeric characters from the string.
// If spaceSeparator is true, it replaces non-alphanumeric characters with a space; otherwise, it replaces them with an underscore.
// It also trims leading and trailing non-alphanumeric characters.
// The function returns an empty string if no alphanumeric characters are found.
func CleanAlphaNumerics(s string, spaceSeparator bool, exclude []rune) string {
	if len(s) == 0 {
		return s
	}
	runes := []rune(s)

	start := AlNumStart(s, exclude)
	end := AlNumEnd(s, exclude)

	if start > end || start < 0 || end < 0 {
		return ""
	}

	result := []rune{}
	prevUnderscore := false
	for i := start; i <= end; i++ {
		if unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || slices.Contains(exclude, runes[i]) {
			result = append(result, runes[i])
			prevUnderscore = false
		} else {
			if !prevUnderscore && i+1 <= end && (unicode.IsLetter(runes[i+1]) || unicode.IsDigit(runes[i+1]) || slices.Contains(exclude, runes[i+1])) {
				if spaceSeparator {
					result = append(result, ' ')
				} else {
					result = append(result, '_')
				}
				prevUnderscore = true
			}
		}
	}

	return string(result)
}

// CleanAlphaNumericsArr applies CleanAlphaNumerics to each string in the slice.
// It returns a new slice with cleaned strings.
// The exclude parameter allows specifying characters that should not be removed.
func CleanAlphaNumericsArr(s []string, exclude []rune) []string {
	if len(s) == 0 {
		return s
	}
	for i, str := range s {
		s[i] = CleanAlphaNumerics(str, true, exclude)
	}
	return s
}

// CleanLeadingTrailing removes leading and trailing non-alphanumeric characters from the string.
// It returns an empty string if no alphanumeric characters are found.
func CleanLeadingTrailing(s string, exclude []rune) string {
	if len(s) == 0 {
		return s
	}
	start := AlNumStart(s, exclude)
	end := AlNumEnd(s, exclude)

	if start > end {
		return ""
	}

	return s[start : end+1]
}

// AlNumStart returns the index of the first alphanumeric character in the string.
// If no alphanumeric character is found, it returns 0.
func AlNumStart(s string, exclude []rune) int {
	start := 0
	runes := []rune(s)
	n := len(runes)
	if n == 0 {
		return -1
	}
	for start < n && !unicode.IsLetter(runes[start]) && !unicode.IsDigit(runes[start]) && !slices.Contains(exclude, runes[start]) {
		start++
	}
	return start
}

// AlNumEnd returns the index of the last alphanumeric character in the string.
// If string is empty or no alphanumeric character is found, it returns -1.
func AlNumEnd(s string, exclude []rune) int {
	if len(s) == 0 {
		return -1
	}

	runes := []rune(s)
	// Find the last alphanumeric character
	end := len(s) - 1

	defer func() {
		if r := recover(); r != nil {
			fmt.Println()
			fmt.Printf("AlNumEnd - recovered processing error:: %v (type: %T), string: %s, num-runes: %d, len-s: %d\n", r, r, s, len(runes), len(s))
			fmt.Println()
		}
	}()

	for end >= 0 && !unicode.IsLetter(runes[end]) && !unicode.IsDigit(runes[end]) && !slices.Contains(exclude, runes[end]) {
		end--
	}
	return end
}

// CleanCSVLineQuotes removes quotes from the beginning and end of each field in a CSV line.
func CleanCSVLineQuotes(line, separator string) string {
	if len(line) == 0 {
		return line
	}

	if separator == "" {
		separator = ","
	}
	split := strings.Split(line, separator)
	for i, f := range split {
		fIdx, lIdx := strings.Index(f, `"`), strings.LastIndex(f, `"`)
		if fIdx >= 0 && lIdx > 0 && fIdx < lIdx && len(f) >= 2 {
			split[i] = f[fIdx+1 : lIdx]
		}
	}
	return strings.Join(split, separator)
}

// CleanHeaders cleans the headers by removing leading/trailing spaces
func CleanHeaders(headers []string) []string {
	if len(headers) == 0 {
		return headers
	}

	for i, header := range headers {
		headers[i] = CleanAlphaNumerics(header, false, []rune{'-', '_'})
	}
	return headers
}

// CleanRecord removes unwanted characters from a record string.
// It replaces asterisks with an empty string and cleans quotes from the record.
// It returns the cleaned record string.
func CleanRecord(recStr string) string {
	if len(recStr) == 0 {
		return recStr
	}

	cleanedStr := strings.ReplaceAll(recStr, "*", "")
	cleanedStr = CleanCSVLineQuotes(cleanedStr, "|")
	return cleanedStr
}

// ReadSingleRecord reads a single record from a string and returns it as a slice of strings.
// It uses a CSV reader with '|' as the delimiter and allows variable number of fields per record.
// If an error occurs during reading, it returns an error.
func ReadSingleRecord(recStr string) ([]string, error) {
	if len(recStr) == 0 {
		return nil, nil
	}

	bReader := bytes.NewReader([]byte(recStr))
	csvReader := csv.NewReader(bReader)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	record, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func ReadSingleRecordFromFile(ctx context.Context, filePath string, offset int64) ([]string, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("StringUtils:ReadSingleRecordFromFile - error getting logger from context: %w", err)
	}

	// Open the local CSV file.
	f, err := os.Open(filePath)
	if err != nil {
		l.Error("error opening local CSV file", "path", filePath, "error", err.Error())
		return nil, err
	}
	defer f.Close()

	// Read data bytes from the file at the specified offset
	data := make([]byte, 800)
	numBytesRead, err := f.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		l.Error("error reading local CSV file", "path", filePath, "offset", offset, "error", err.Error())
		return nil, fmt.Errorf("error reading file %s at offset %d: %w", filePath, offset, err)
	}

	i := bytes.LastIndex(data, []byte{'\n'})
	if i >= 0 {
		numBytesRead = i + 1
	}

	l.Debug("read bytes from file", "path", filePath, "offset", offset, "num-bytes-read", numBytesRead)
	l.Debug("data read", "data", string(data[:numBytesRead]))

	bReader := bytes.NewReader(data)
	csvReader := csv.NewReader(bReader)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	nextOffset := csvReader.InputOffset()

	record, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	cleanedStr := CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
	record, err = ReadSingleRecord(cleanedStr)
	if err != nil {
		return nil, err
	}

	for {
		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
		}

		cleanedStr := CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
		rec, err = ReadSingleRecord(cleanedStr)
		if err != nil {
			l.Error("error reading additional record", "error", err.Error())
		}
		l.Debug("Read additional record", "record", rec)
		nextOffset = csvReader.InputOffset()
	}

	return record, nil
}

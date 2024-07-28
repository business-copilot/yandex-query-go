package yq

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

type Results struct {
	rawResults map[string]interface{}
	results    map[string]interface{}
}

func NewYQResults(results map[string]interface{}) *Results {
	return &Results{
		rawResults: results,
		results:    nil,
	}
}

func (r *Results) convert() {
	if r.results != nil {
		return
	}

	columns := r.rawResults["columns"].([]interface{})
	rows := r.rawResults["rows"].([]interface{})

	converters := make([]func(interface{}) interface{}, len(columns))
	for i, col := range columns {
		colType := col.(map[string]interface{})["type"].(string)
		converters[i] = r.getConverter(colType)
	}

	convertedRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		convertedRow := make([]interface{}, len(converters))
		for j, value := range row.([]interface{}) {
			convertedRow[j] = converters[j](value)
		}
		convertedRows[i] = convertedRow
	}

	r.results = map[string]interface{}{
		"rows":    convertedRows,
		"columns": columns,
	}
}

func (r *Results) getConverter(columnType string) func(interface{}) interface{} {
	switch columnType {
	case "Int8", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64", "Bool", "Utf8", "Uuid", "Void", "Null", "EmptyList", "Struct<>", "Tuple<>":
		return func(v interface{}) interface{} { return v }
	case "String":
		return r.convertFromBase64
	case "Float", "Double":
		return r.convertFromFloat
	case "Date", "Datetime", "Timestamp":
		return r.convertFromDatetime
	// Implement other type conversions as needed
	default:
		return func(v interface{}) interface{} { return v }
	}
}

func (r *Results) convertFromBase64(value interface{}) interface{} {
	str, ok := value.(string)
	if !ok {
		return value
	}
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return value
	}
	return string(decoded)
}

func (r *Results) convertFromFloat(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return v
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return value
		}
		return f
	default:
		return value
	}
}

func (r *Results) convertFromDatetime(value interface{}) interface{} {
	str, ok := value.(string)
	if !ok {
		return value
	}
	t, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return value
	}
	return t
}

func (r *Results) Results() map[string]interface{} {
	r.convert()
	return r.results
}

func (r *Results) RawResults() map[string]interface{} {
	return r.rawResults
}

func (r *Results) ToTable() [][]interface{} {
	r.convert()
	return r.results["rows"].([][]interface{})
}

func (r *Results) String() string {
	r.convert()
	return fmt.Sprintf("%v", r.results)
}

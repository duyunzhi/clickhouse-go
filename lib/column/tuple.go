package column

import (
	"bytes"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Tuple struct {
	base
	columns []Column
	buffers []*Buffer
}

func (tuple *Tuple) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Tuple(T) column")
}

func (tuple *Tuple) ReadTuple(decoder *binary.Decoder, rows int) ([]interface{}, error) {
	var values = make([][]interface{}, rows)

	for _, c := range tuple.columns {

		switch column := c.(type) {
		case *Array:
			cols, err := column.ReadArray(decoder, rows)
			if err != nil {
				return nil, err
			}
			for i := 0; i < rows; i++ {
				values[i] = append(values[i], cols[i])
			}

		case *Nullable:
			cols, err := column.ReadNull(decoder, rows)
			if err != nil {
				return nil, err
			}
			for i := 0; i < rows; i++ {
				values[i] = append(values[i], cols[i])
			}

		case *Tuple:
			cols, err := column.ReadTuple(decoder, rows)
			if err != nil {
				return nil, err
			}
			for i := 0; i < rows; i++ {
				values[i] = append(values[i], cols[i])
			}

		default:
			for i := 0; i < rows; i++ {
				value, err := c.Read(decoder, false)
				if err != nil {
					return nil, err
				}
				values[i] = append(values[i], value)
			}
		}
	}

	var ret = make([]interface{}, rows)
	for i := range values {
		ret[i] = values[i]
	}

	return ret, nil
}

func (tuple *Tuple) Write(encoder *binary.Encoder, v interface{}) (err error) {
	tuple.reserve()
	value := reflect.ValueOf(v)
	for i, column := range tuple.columns {
		err := column.Write(tuple.buffers[i].Column, value.Index(i).Interface())
		if err != nil {
			return err
		}
	}
	return nil
}

func (tuple *Tuple) GetBuffers() (buffers []*Buffer) {
	return tuple.buffers
}

func (tuple *Tuple) reserve() {
	if len(tuple.buffers) == 0 {
		tuple.buffers = make([]*Buffer, len(tuple.columns))
		for i := 0; i < len(tuple.columns); i++ {
			var (
				columnBuffer = new(bytes.Buffer)
			)
			tuple.buffers[i] = &Buffer{
				Column:       binary.NewEncoder(columnBuffer),
				ColumnBuffer: columnBuffer,
			}
		}
	}
}

func parseTuple(name, chType string, timezone *time.Location) (Column, error) {
	var columnType = chType

	chType = chType[6 : len(chType)-1]
	var types []string
	var last, diff int
	for i, b := range chType + "," {
		if b == '(' {
			diff++
		} else if b == ')' {
			diff--
		} else if b == ',' && diff == 0 {
			types = append(types, chType[last:i])
			last = i + 2
		}
	}

	var columns = make([]Column, 0, len(types))
	for i, chType := range types {
		fieldName := name + "." + strconv.Itoa(i+1)
		if !tupleType(chType) {
			types := strings.Fields(chType)
			if len(types) == 2 {
				fieldName = name + "." + types[0]
				chType = types[1]
			}
		}
		println(chType)
		column, err := Factory(fieldName, chType, timezone)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", chType, err)
		}
		columns = append(columns, column)
	}

	return &Tuple{
		base: base{
			name:    name,
			chType:  columnType,
			valueOf: reflect.ValueOf([]interface{}{}),
		},
		columns: columns,
	}, nil
}

func tupleType(chType string) bool {
	switch chType {
	case "Int8":
		return true
	case "Int16":
		return true
	case "Int32":
		return true
	case "Int64":
		return true
	case "UInt8":
		return true
	case "UInt16":
		return true
	case "UInt32":
		return true
	case "UInt64":
		return true
	case "Float32":
		return true
	case "Float64":
		return true
	case "String":
		return true
	case "UUID":
		return true
	case "Date":
		return true
	case "IPv4":
		return true
	case "IPv6":
		return true
	}
	return false
}

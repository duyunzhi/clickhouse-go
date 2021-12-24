package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type CkMap struct {
	base
	columns []Column
}

func (ckMap *CkMap) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Map column")
}

func (ckMap *CkMap) ReadMap(decoder *binary.Decoder, rows int) ([]interface{}, error) {
	return nil, fmt.Errorf("not supprot read map column")
}

func (ckMap *CkMap) Write(encoder *binary.Encoder, v interface{}) (err error) {
	m, ok := v.(map[string]string)
	if ok {
		i := 0
		values := make([]string, len(m))
		for key, value := range m {
			err := encoder.String(key)
			if err != nil {
				println(err)
			}
			values[i] = value
			i++
		}
		for _, value := range values {
			err := encoder.String(value)
			if err != nil {
				println(err)
			}
		}
	}
	return nil
}

func parseMap(name, chType string, timezone *time.Location) (Column, error) {
	var columnType = chType

	chType = chType[4 : len(chType)-1]
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
		column, err := Factory(fieldName, chType, timezone)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", chType, err)
		}
		columns = append(columns, column)
	}
	return &CkMap{
		base: base{
			name:    name,
			chType:  columnType,
			valueOf: reflect.ValueOf([]interface{}{}),
		},
		columns: columns,
	}, nil
}

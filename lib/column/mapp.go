package column

import (
	"bytes"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"reflect"
	"time"
)

type CkMap struct {
	base
	columns []Column
	Buffers []*Buffer
}

func (ckMap *CkMap) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Map column")
}

func (ckMap *CkMap) ReadMap(decoder *binary.Decoder, rows int) ([]interface{}, error) {
	return nil, fmt.Errorf("not supprot read map column")
}

func (ckMap *CkMap) Write(encoder *binary.Encoder, v interface{}) (err error) {
	ckMap.reserve()
	m, ok := v.(map[interface{}]interface{})
	if ok {
		i := 0
		values := make([]interface{}, len(m))
		for key, value := range m {
			err := ckMap.columns[0].Write(ckMap.Buffers[0].Column, key)
			if err != nil {
				return err
			}
			values[i] = value
			i++
		}
		for _, value := range values {
			err := ckMap.columns[1].Write(ckMap.Buffers[1].Column, value)
			if err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("not support Map type[%T], require type[map[interface{}]interface{}]", v)
	}
	return nil
}

func (ckMap *CkMap) reserve() {
	if len(ckMap.Buffers) == 0 {
		ckMap.Buffers = make([]*Buffer, len(ckMap.columns))
		for i := 0; i < len(ckMap.columns); i++ {
			var (
				columnBuffer = new(bytes.Buffer)
			)
			ckMap.Buffers[i] = &Buffer{
				Column:       binary.NewEncoder(columnBuffer),
				ColumnBuffer: columnBuffer,
			}
		}
	}
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
	if len(types) != 2 {
		return nil, fmt.Errorf("must (K, V)")
	}
	for i, chType := range types {
		fieldName := name
		if i == 0 {
			fieldName = name + ".k"
		} else if i == 1 {
			fieldName = name + ".v"
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

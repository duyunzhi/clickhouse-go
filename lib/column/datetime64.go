package column

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type DateTime64 struct {
	base
	Timezone *time.Location
}

func (dt *DateTime64) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	value, err := decoder.Int64()
	if err != nil {
		return nil, err
	}

	precision, err := dt.getPrecision()
	if err != nil {
		return nil, err
	}

	var nano int64
	if precision < 19 {
		nano = value * int64(math.Pow10(9-precision))
	}

	sec := nano / int64(10e8)
	nsec := nano - sec*10e8

	return time.Unix(sec, nsec).In(dt.Timezone), nil
}

func (dt *DateTime64) Write(encoder *binary.Encoder, v interface{}) error {
	var nanosecondLen = 19
	var timestamp int64
	switch value := v.(type) {
	case time.Time:
		if !value.IsZero() {
			timestamp = value.UnixNano()
		}
	case uint64:
		timestamp = int64(value)
	case int64:
		timestamp = value
	case string:
		var err error
		timestamp, err = dt.parse(value)
		if err != nil {
			return err
		}
	case *time.Time:
		if value != nil && !(*value).IsZero() {
			timestamp = (*value).UnixNano()
		}
	case *int64:
		timestamp = *value
	case *string:
		var err error
		timestamp, err = dt.parse(*value)
		if err != nil {
			return err
		}
	default:
		return &ErrUnexpectedType{
			T:      v,
			Column: dt,
		}
	}

	tsSize := len(strconv.FormatInt(timestamp, 10))
	if tsSize != nanosecondLen {
		if tsSize > nanosecondLen {
			return fmt.Errorf("illegal time: %v", v)
		}
		differ := nanosecondLen - tsSize
		timestamp = timestamp * int64(math.Pow10(differ))
	}

	precision, err := dt.getPrecision()
	if err != nil {
		return err
	}
	pow := int64(math.Pow10(9 - precision))

	timestamp = timestamp / pow
	return encoder.Int64(timestamp)
}

func (dt *DateTime64) parse(value string) (int64, error) {
	tv, err := time.ParseInLocation("2006-01-02 15:04:05.999", value, time.Local)
	if err != nil {
		return 0, err
	}
	return tv.UnixNano(), nil
}

func (dt *DateTime64) getPrecision() (int, error) {
	dtParams := dt.base.chType[11 : len(dt.base.chType)-1]
	precision, err := strconv.Atoi(strings.Split(dtParams, ",")[0])
	if err != nil {
		return 0, err
	}
	return precision, nil
}

func (dt *DateTime64) getLen(precision int) int {
	return 11 + precision
}

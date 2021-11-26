package mig

import "time"

// ValueType the type for JSON element
type ValueType int

const (
	// InvalidValue invalid JSON element
	InvalidValue ValueType = iota
	// StringValue JSON element "string"
	StringValue
	// NumberValue JSON element 100 or 0.10
	NumberValue
	// NilValue JSON element null
	NilValue
	// BoolValue JSON element true or false
	BoolValue
	// ArrayValue JSON element []
	ArrayValue
	// ObjectValue JSON element {}
	ObjectValue
)

// Any generic object representation.
// The lazy json implementation holds []byte and parse lazily.
type Any interface {
	ToBool() bool
	ToInt() int
	ToUint() uint
	ToFloat64() float64
	ToString() string
	ToTime() time.Time
}

type MAny struct {
	v interface{}
}

func (any *MAny) ToBool() bool {
	if v, ok := any.v.(bool); ok {
		return v
	} else {
		return false
	}
}

func (any *MAny) ToInt() int {
	switch any.v.(type) {
	case int:
		return any.v.(int)
	case int8:
		return int(any.v.(int8))
	case int16:
		return int(any.v.(int16))
	case int32:
		return int(any.v.(int32))
	case int64:
		return int(any.v.(int64))
	}
	return 0
}

func (any *MAny) ToUint() uint {
	switch any.v.(type) {
	case int:
		return any.v.(uint)
	case int8:
		return uint(any.v.(uint8))
	case int16:
		return uint(any.v.(uint16))
	case int32:
		return uint(any.v.(uint32))
	case int64:
		return uint(any.v.(uint64))
	}
	return 0
}

func (any *MAny) ToFloat64() float64 {
	switch any.v.(type) {
	case float32:
		return float64(any.v.(float32))
	case float64:
		return any.v.(float64)
	}
	return 0
}

func (any *MAny) ToString() string {
	if v, ok := any.v.(string); ok {
		return v
	} else {
		return ""
	}
}

func (any *MAny) ToTime() time.Time {
	if v, ok := any.v.(time.Time); ok {
		return v
	} else {
		return time.Now()
	}
}

func GetMapAny(path string, m map[string]interface{}) Any {
	var v interface{}

	if mv, ok := m[path]; ok {
		v = mv
	} else {
		v = nil
	}
	return &MAny{
		v: v,
	}
}

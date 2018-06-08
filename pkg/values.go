package neo4j

/*
#cgo CFLAGS: -ID:/Projects.GitHub/seabolt/seabolt/include -I/home/ali/Projects/neo4j/seabolt/seabolt/include
#cgo LDFLAGS: -LD:/Projects.GitHub/seabolt/build/lib/debug -L/home/ali/Projects/neo4j/seabolt/build/lib -lseabolt

#include <stdlib.h>

#include "bolt/values.h"
*/
import "C"
import (
	"errors"
	"unsafe"
	"reflect"
)

func valueAsGo(value *C.struct_BoltValue) (interface{}, error) {
	switch {
	case value._type == C.BOLT_NULL:
		return nil, nil
	case value._type == C.BOLT_BOOLEAN:
		return valueAsBoolean(value), nil
	case value._type == C.BOLT_INTEGER:
		return valueAsInt(value), nil
	case value._type == C.BOLT_FLOAT:
		return valueAsFloat(value), nil
	case value._type == C.BOLT_STRING:
		return valueAsString(value), nil
	case value._type == C.BOLT_DICTIONARY:
		return valueAsDictionary(value), nil
	case value._type == C.BOLT_LIST:
		return valueAsList(value), nil
	case value._type == C.BOLT_BYTES:
		return valueAsBytes(value), nil
	}

	return nil, errors.New("unexpected data type")
}

func valueAsBoolean(value *C.struct_BoltValue) bool {
	val := C.BoltBoolean_get(value)
	return val == 1
}

func valueAsInt(value *C.struct_BoltValue) int64 {
	val := C.BoltInteger_get(value)
	return int64(val)
}

func valueAsFloat(value *C.struct_BoltValue) float64 {
	val := C.BoltFloat_get(value)
	return float64(val)
}

func valueAsString(value *C.struct_BoltValue) string {
	val := C.BoltString_get(value)
	return C.GoStringN(val, C.int(value.size))
}

func valueAsDictionary(value *C.struct_BoltValue) map[string]interface{} {
	size := int(value.size)
	dict := make(map[string]interface{}, size)
	for i := 0; i < size; i++ {
		index := C.int32_t(i)
		key := valueAsString(C.BoltDictionary_key(value, index))
		value, err := valueAsGo(C.BoltDictionary_value(value, index))
		if err != nil {
			panic(err)
		}

		dict[key] = value
	}
	return dict
}

func valueAsList(value *C.struct_BoltValue) []interface{} {
	size := int(value.size)
	list := make([]interface{}, size)
	for i := 0; i < size; i++ {
		index := C.int32_t(i)
		value, err := valueAsGo(C.BoltList_value(value, index))
		if err != nil {
			panic(err)
		}

		list[i] = value
	}
	return list
}

func valueAsBytes(value *C.struct_BoltValue) []byte {
	val := C.BoltBytes_get_all(value)
	return C.GoBytes(unsafe.Pointer(val), C.int(value.size))
}

func valueToConnector(value interface{}) *C.struct_BoltValue {
	res := C.BoltValue_create()

	valueAsConnector(res, value)

	return res
}

func valueAsConnector(target *C.struct_BoltValue, value interface{}) {
	if value == nil {
		C.BoltValue_format_as_Null(target)
		return
	}

	handled := true
	switch v := value.(type) {
	case bool:
		boolAsValue(target, v)
	case int8:
		intAsValue(target, int64(v))
	case int16:
		intAsValue(target, int64(v))
	case int:
		intAsValue(target, int64(v))
	case int32:
		intAsValue(target, int64(v))
	case int64:
		intAsValue(target, v)
	case uint8:
		intAsValue(target, int64(v))
	case uint16:
		intAsValue(target, int64(v))
	case uint:
		intAsValue(target, int64(v))
	case uint32:
		intAsValue(target, int64(v))
	case uint64:
		intAsValue(target, int64(v))
	case float32:
		floatAsValue(target, float64(v))
	case float64:
		floatAsValue(target, v)
	case string:
		stringAsValue(target, v)
	default:
		handled = false
	}

	if !handled {
		v := reflect.TypeOf(value)

		handled = true
		switch v.Kind() {
		case reflect.Ptr:
			valueAsConnector(target, reflect.ValueOf(value).Elem().Interface())
		case reflect.Slice:
			listAsValue(target, value)
		case reflect.Map:
			mapAsValue(target, value)
		default:
			handled = false
		}
	}

	if !handled {
		panic("not supported value for conversion")
	}
}

func boolAsValue(target *C.struct_BoltValue, value bool) {
	data := C.char(0)
	if value {
		data = C.char(1)
	}

	C.BoltValue_format_as_Boolean(target, data)
}

func intAsValue(target *C.struct_BoltValue, value int64) {
	C.BoltValue_format_as_Integer(target, C.int64_t(value))
}

func floatAsValue(target *C.struct_BoltValue, value float64) {
	C.BoltValue_format_as_Float(target, C.double(value))
}

func stringAsValue(target *C.struct_BoltValue, value string) {
	str := C.CString(value)
	C.BoltValue_format_as_String(target, str, C.int32_t(len(value)))
	C.free(unsafe.Pointer(str))
}

func listAsValue(target *C.struct_BoltValue, value interface{}) {
	slice := reflect.ValueOf(value)
	if slice.Kind() != reflect.Slice {
		panic("listAsValue invoked with a non-slice type")
	}

	C.BoltValue_format_as_List(target, C.int32_t(slice.Len()))
	for i := 0; i < slice.Len(); i++ {
		elTarget := C.BoltList_value(target, C.int32_t(i))
		valueAsConnector(elTarget, slice.Index(i).Interface())
	}
}

func mapAsValue(target *C.struct_BoltValue, value interface{}) {
	dict := reflect.ValueOf(value)
	if dict.Kind() != reflect.Map {
		panic("mapAsValue invoked with a non-map type")
	}

	C.BoltValue_format_as_Dictionary(target, C.int32_t(dict.Len()))

	index := C.int32_t(0)
	for _, key := range dict.MapKeys() {
		keyTarget := C.BoltDictionary_key(target, index)
		elTarget := C.BoltDictionary_value(target, index)

		valueAsConnector(keyTarget, key.Interface())
		valueAsConnector(elTarget, dict.MapIndex(key).Interface())

		index += 1
	}
}
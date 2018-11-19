/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobolt

/*
#include <stdlib.h>

#include "bolt/bolt.h"
*/
import "C"
import (
	"reflect"
	"unsafe"
)

type boltValueSystem struct {
	valueHandlers            []ValueHandler
	valueHandlersBySignature map[int16]ValueHandler
	valueHandlersByType      map[reflect.Type]ValueHandler
	connectorErrorFactory    func(state, code int, codeText, context, description string) ConnectorError
	databaseErrorFactory     func(classification, code, message string) DatabaseError
	genericErrorFactory      func(format string, args ...interface{}) GenericError
}

func (valueSystem *boltValueSystem) valueAsGo(value *C.struct_BoltValue) (interface{}, error) {
	valueType := C.BoltValue_type(value)

	switch {
	case valueType == C.BOLT_NULL:
		return nil, nil
	case valueType == C.BOLT_BOOLEAN:
		return valueSystem.valueAsBoolean(value), nil
	case valueType == C.BOLT_INTEGER:
		return valueSystem.valueAsInt(value), nil
	case valueType == C.BOLT_FLOAT:
		return valueSystem.valueAsFloat(value), nil
	case valueType == C.BOLT_STRING:
		return valueSystem.valueAsString(value), nil
	case valueType == C.BOLT_DICTIONARY:
		return valueSystem.valueAsDictionary(value)
	case valueType == C.BOLT_LIST:
		return valueSystem.valueAsList(value)
	case valueType == C.BOLT_BYTES:
		return valueSystem.valueAsBytes(value), nil
	case valueType == C.BOLT_STRUCTURE:
		signature := int16(C.BoltStructure_code(value))

		if handler, ok := valueSystem.valueHandlersBySignature[signature]; ok {
			listValue, err := valueSystem.structAsList(value)
			if err != nil {
				return nil, err
			}

			return handler.Read(signature, listValue)
		}

		return nil, newGenericError("unsupported struct type received: %#x", signature)
	}

	return nil, newGenericError("unsupported data type")
}

func (valueSystem *boltValueSystem) valueAsBoolean(value *C.struct_BoltValue) bool {
	val := C.BoltBoolean_get(value)
	return val == 1
}

func (valueSystem *boltValueSystem) valueAsInt(value *C.struct_BoltValue) int64 {
	val := C.BoltInteger_get(value)
	return int64(val)
}

func (valueSystem *boltValueSystem) valueAsFloat(value *C.struct_BoltValue) float64 {
	val := C.BoltFloat_get(value)
	return float64(val)
}

func (valueSystem *boltValueSystem) valueAsString(value *C.struct_BoltValue) string {
	val := C.BoltString_get(value)
	return C.GoStringN(val, C.BoltValue_size(value))
}

func (valueSystem *boltValueSystem) valueAsDictionary(value *C.struct_BoltValue) (map[string]interface{}, error) {
	size := int(C.BoltValue_size(value))
	dict := make(map[string]interface{}, size)
	for i := 0; i < size; i++ {
		index := C.int32_t(i)
		key := valueSystem.valueAsString(C.BoltDictionary_key(value, index))
		value, err := valueSystem.valueAsGo(C.BoltDictionary_value(value, index))
		if err != nil {
			return nil, err
		}

		dict[key] = value
	}
	return dict, nil
}

func (valueSystem *boltValueSystem) valueAsList(value *C.struct_BoltValue) ([]interface{}, error) {
	size := int(C.BoltValue_size(value))
	list := make([]interface{}, size)
	for i := 0; i < size; i++ {
		index := C.int32_t(i)
		value, err := valueSystem.valueAsGo(C.BoltList_value(value, index))
		if err != nil {
			return nil, err
		}

		list[i] = value
	}
	return list, nil
}

func (valueSystem *boltValueSystem) structAsList(value *C.struct_BoltValue) ([]interface{}, error) {
	size := int(C.BoltValue_size(value))
	list := make([]interface{}, size)
	for i := 0; i < size; i++ {
		index := C.int32_t(i)
		value, err := valueSystem.valueAsGo(C.BoltStructure_value(value, index))
		if err != nil {
			return nil, err
		}

		list[i] = value
	}
	return list, nil
}

func (valueSystem *boltValueSystem) valueAsBytes(value *C.struct_BoltValue) []byte {
	val := C.BoltBytes_get_all(value)
	return C.GoBytes(unsafe.Pointer(val), C.BoltValue_size(value))
}

func (valueSystem *boltValueSystem) valueToConnector(value interface{}) *C.struct_BoltValue {
	res := C.BoltValue_create()

	valueSystem.valueAsConnector(res, value)

	return res
}

func (valueSystem *boltValueSystem) valueAsConnector(target *C.struct_BoltValue, value interface{}) error {
	if value == nil {
		C.BoltValue_format_as_Null(target)
	} else {
		handled := true
		switch v := value.(type) {
		case bool:
			valueSystem.boolAsValue(target, v)
		case int8:
			valueSystem.intAsValue(target, int64(v))
		case int16:
			valueSystem.intAsValue(target, int64(v))
		case int:
			valueSystem.intAsValue(target, int64(v))
		case int32:
			valueSystem.intAsValue(target, int64(v))
		case int64:
			valueSystem.intAsValue(target, v)
		case uint8:
			valueSystem.intAsValue(target, int64(v))
		case uint16:
			valueSystem.intAsValue(target, int64(v))
		case uint:
			valueSystem.intAsValue(target, int64(v))
		case uint32:
			valueSystem.intAsValue(target, int64(v))
		case uint64:
			valueSystem.intAsValue(target, int64(v))
		case float32:
			valueSystem.floatAsValue(target, float64(v))
		case float64:
			valueSystem.floatAsValue(target, v)
		case string:
			valueSystem.stringAsValue(target, v)
		case []byte:
			valueSystem.bytesAsValue(target, v)
		default:
			handled = false
		}

		if !handled {
			v := reflect.TypeOf(value)

			handled = true
			switch v.Kind() {
			case reflect.Ptr:
				ptrv := reflect.ValueOf(value)
				if ptrv.IsNil() {
					C.BoltValue_format_as_Null(target)
				} else {
					valueSystem.valueAsConnector(target, ptrv.Elem().Interface())
				}
			case reflect.Slice:
				valueSystem.listAsValue(target, value)
			case reflect.Map:
				valueSystem.mapAsValue(target, value)
			default:
				// ask for value handlers
				if handler, ok := valueSystem.valueHandlersByType[v]; ok {
					signature, fields, err := handler.Write(value)
					if err != nil {
						return err
					}

					C.BoltValue_format_as_Structure(target, C.int16_t(signature), C.int32_t(len(fields)))
					for index, fieldValue := range fields {
						valueSystem.valueAsConnector(C.BoltStructure_value(target, C.int32_t(index)), fieldValue)
					}
				} else {
					handled = false
				}
			}
		}

		if !handled {
			return newGenericError("unsupported value for conversion: %v", value)
		}
	}

	return nil
}

func (valueSystem *boltValueSystem) boolAsValue(target *C.struct_BoltValue, value bool) {
	data := C.char(0)
	if value {
		data = C.char(1)
	}

	C.BoltValue_format_as_Boolean(target, data)
}

func (valueSystem *boltValueSystem) intAsValue(target *C.struct_BoltValue, value int64) {
	C.BoltValue_format_as_Integer(target, C.int64_t(value))
}

func (valueSystem *boltValueSystem) floatAsValue(target *C.struct_BoltValue, value float64) {
	C.BoltValue_format_as_Float(target, C.double(value))
}

func (valueSystem *boltValueSystem) stringAsValue(target *C.struct_BoltValue, value string) {
	str := C.CString(value)
	C.BoltValue_format_as_String(target, str, C.int32_t(len(value)))
	C.free(unsafe.Pointer(str))
}

func (valueSystem *boltValueSystem) bytesAsValue(target *C.struct_BoltValue, value []byte) {
	bytes := C.CBytes(value)
	str := (*C.char)(bytes)
	C.BoltValue_format_as_Bytes(target, str, C.int32_t(len(value)))
	C.free(bytes)
}

func (valueSystem *boltValueSystem) listAsValue(target *C.struct_BoltValue, value interface{}) error {
	slice := reflect.ValueOf(value)
	if slice.Kind() != reflect.Slice {
		return newGenericError("listAsValue invoked with a non-slice type: %v", value)
	}

	C.BoltValue_format_as_List(target, C.int32_t(slice.Len()))
	for i := 0; i < slice.Len(); i++ {
		elTarget := C.BoltList_value(target, C.int32_t(i))
		valueSystem.valueAsConnector(elTarget, slice.Index(i).Interface())
	}

	return nil
}

func (valueSystem *boltValueSystem) mapAsValue(target *C.struct_BoltValue, value interface{}) error {
	dict := reflect.ValueOf(value)
	if dict.Kind() != reflect.Map {
		return newGenericError("mapAsValue invoked with a non-map type: %v", value)
	}

	C.BoltValue_format_as_Dictionary(target, C.int32_t(dict.Len()))

	index := C.int32_t(0)
	for _, key := range dict.MapKeys() {
		keyTarget := C.BoltDictionary_key(target, index)
		elTarget := C.BoltDictionary_value(target, index)

		valueSystem.valueAsConnector(keyTarget, key.Interface())
		valueSystem.valueAsConnector(elTarget, dict.MapIndex(key).Interface())

		index++
	}

	return nil
}

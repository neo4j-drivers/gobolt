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

package seabolt

/*
#cgo pkg-config: seabolt

#include <stdlib.h>

#include "bolt/values.h"
*/
import "C"
import (
	"errors"
	"reflect"
	"unsafe"
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
	case value._type == C.BOLT_STRUCTURE:
		switch value.subtype {
		case C.BOLT_VALUE_TYPE_NODE:
			return valueAsNode(value), nil
		case C.BOLT_VALUE_TYPE_RELATIONSHIP:
			return valueAsRelationship(value), nil
		case C.BOLT_VALUE_TYPE_UNBOUND_RELATIONSHIP:
			return valueAsUnboundRelationship(value), nil
		case C.BOLT_VALUE_TYPE_PATH:
			return valueAsPath(value), nil
		}
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

func valueAsNode(value *C.struct_BoltValue) NodeValue {
	idValue := C.BoltStructure_value(value, 0)
	labelsValue := C.BoltStructure_value(value, 1)
	propsValue := C.BoltStructure_value(value, 2)

	labelsSize := int(labelsValue.size)
	labels := make([]string, labelsValue.size)
	for i := 0; i < labelsSize; i++ {
		labelValue := C.BoltList_value(labelsValue, C.int32_t(i))
		label := valueAsString(labelValue)
		labels[i] = label
	}

	props := valueAsDictionary(propsValue)

	return NodeValue{
		id:     valueAsInt(idValue),
		labels: labels,
		props:  props,
	}
}

func valueAsRelationship(value *C.struct_BoltValue) RelationshipValue {
	idValue := C.BoltStructure_value(value, 0)
	startIDValue := C.BoltStructure_value(value, 1)
	endIDValue := C.BoltStructure_value(value, 2)
	relTypeValue := C.BoltStructure_value(value, 3)
	propsValue := C.BoltStructure_value(value, 4)

	return RelationshipValue{
		id:      valueAsInt(idValue),
		startID: valueAsInt(startIDValue),
		endID:   valueAsInt(endIDValue),
		relType: valueAsString(relTypeValue),
		props:   valueAsDictionary(propsValue),
	}
}

func valueAsUnboundRelationship(value *C.struct_BoltValue) RelationshipValue {
	idValue := C.BoltStructure_value(value, 0)
	relTypeValue := C.BoltStructure_value(value, 1)
	propsValue := C.BoltStructure_value(value, 2)

	return RelationshipValue{
		id:      valueAsInt(idValue),
		startID: -1,
		endID:   -1,
		relType: valueAsString(relTypeValue),
		props:   valueAsDictionary(propsValue),
	}
}

func valueAsPath(value *C.struct_BoltValue) PathValue {
	uniqueNodesValue := C.BoltStructure_value(value, 0)
	uniqueRelsValue := C.BoltStructure_value(value, 1)
	segmentsValue := C.BoltStructure_value(value, 2)

	uniqueNodesSize := int(uniqueNodesValue.size)
	uniqueNodes := make([]NodeValue, uniqueNodesSize)
	for i := 0; i < uniqueNodesSize; i++ {
		uniqueNodes[i] = valueAsNode(C.BoltList_value(uniqueNodesValue, C.int32_t(i)))
	}

	uniqueRelsSize := int(uniqueRelsValue.size)
	uniqueRels := make([]RelationshipValue, uniqueRelsSize)
	for i := 0; i < uniqueRelsSize; i++ {
		uniqueRels[i] = valueAsRelationship(C.BoltList_value(uniqueNodesValue, C.int32_t(i)))
	}

	segmentsSize := int(segmentsValue.size) / 2
	segments := make([]SegmentValue, segmentsSize)
	nodes := make([]NodeValue, segmentsSize+1)
	rels := make([]RelationshipValue, segmentsSize)

	prevNode := uniqueNodes[0]
	nodes[0] = prevNode
	for i := 0; i < segmentsSize; i++ {
		relID := valueAsInt(C.BoltList_value(segmentsValue, C.int32_t(2*i)))
		nextNodeIndex := valueAsInt(C.BoltList_value(segmentsValue, C.int32_t(2*i+1)))
		nextNode := uniqueNodes[nextNodeIndex]

		var rel RelationshipValue
		if relID < 0 {
			rel = uniqueRels[(-relID)-1]
			rel.startID = prevNode.id
			rel.endID = nextNode.id
		} else {
			rel = uniqueRels[relID-1]
			rel.startID = prevNode.id
			rel.endID = nextNode.id
		}

		nodes[i+1] = nextNode
		rels[i] = rel
		segments[i] = SegmentValue{start: prevNode, relationship: rel, end: nextNode}
		prevNode = nextNode
	}

	return PathValue{segments: segments, nodes: nodes, relationships: rels}
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

		index++
	}
}

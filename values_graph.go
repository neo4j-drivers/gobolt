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

package neo4j_go_connector

type NodeValue struct {
    id     int64
    labels []string
    props  map[string]interface{}
}

type RelationshipValue struct {
    id      int64
    startId int64
    endId   int64
    relType string
    props   map[string]interface{}
}

type SegmentValue struct {
    start NodeValue
    relationship RelationshipValue
    end NodeValue
}

type PathValue struct {
    segments []SegmentValue
    nodes         []NodeValue
    relationships []RelationshipValue
}

func (node *NodeValue) Id() int64 {
    return node.id
}

func (node *NodeValue) Labels() []string {
    return node.labels
}

func (node *NodeValue) Props() map[string]interface{} {
    return node.props
}

func (rel *RelationshipValue) Id() int64 {
    return rel.id
}

func (rel *RelationshipValue) StartId() int64 {
    return rel.startId
}

func (rel *RelationshipValue) EndId() int64 {
    return rel.endId
}

func (rel *RelationshipValue) Type() string {
    return rel.relType
}

func (rel *RelationshipValue) Props() map[string]interface{} {
    return rel.props
}

func (path *PathValue) Nodes() []NodeValue {
    return path.nodes
}

func (path *PathValue) Relationships() []RelationshipValue {
    return path.relationships
}

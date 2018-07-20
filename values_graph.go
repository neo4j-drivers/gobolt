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

// NodeValue represents a node in the neo4j graph database
type NodeValue struct {
	id     int64
	labels []string
	props  map[string]interface{}
}

// RelationshipValue represents a relationship in the neo4j graph database
type RelationshipValue struct {
	id      int64
	startId int64
	endId   int64
	relType string
	props   map[string]interface{}
}

// SegmentValue represents a relationship with start and end nodes in the neo4j graph database
type SegmentValue struct {
	start        *NodeValue
	relationship *RelationshipValue
	end          *NodeValue
}

// PathValue represents a path of nodes connected with relationships in the neo4j graph database
type PathValue struct {
	segments      []*SegmentValue
	nodes         []*NodeValue
	relationships []*RelationshipValue
}

// ID returns id of the node
func (node *NodeValue) Id() int64 {
	return node.id
}

// Labels returns labels of the node
func (node *NodeValue) Labels() []string {
	return node.labels
}

// Props returns properties of the node
func (node *NodeValue) Props() map[string]interface{} {
	return node.props
}

// ID returns id of the relationship
func (rel *RelationshipValue) Id() int64 {
	return rel.id
}

// StartID returns the id of the start node
func (rel *RelationshipValue) StartId() int64 {
	return rel.startId
}

// EndID returns the id of the end node
func (rel *RelationshipValue) EndId() int64 {
	return rel.endId
}

// Type returns the relationship tyoe
func (rel *RelationshipValue) Type() string {
	return rel.relType
}

// Props returns properties of the relationship
func (rel *RelationshipValue) Props() map[string]interface{} {
	return rel.props
}

// Nodes returns the ordered list of nodes available on the path
func (path *PathValue) Nodes() []*NodeValue {
	return path.nodes
}

// Relationships returns the ordered list of relationships on the path
func (path *PathValue) Relationships() []*RelationshipValue {
	return path.relationships
}

// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"github.com/boltmq/common/message"
)

// OffsetStore: offset存储接口
type OffsetStore interface {
	Load()
	// Persist all offsets,may be in local storage or remote name server
	// Set<MessageQueue>
	PersistAll(mqs []*message.MessageQueue)

	// Persist the offset,may be in local storage or remote name server
	Persist(mq *message.MessageQueue)

	// Remove offset
	RemoveOffset(mq *message.MessageQueue)

	// Get offset from local storage
	ReadOffset(mq *message.MessageQueue, rType ReadOffsetType) int64
	// Update the offset,store it in memory
	UpdateOffset(mq *message.MessageQueue, offset int64, increaseOnly bool)
}

type ReadOffsetType int

const (
	READ_FROM_MEMORY ReadOffsetType = iota
	READ_FROM_STORE
	MEMORY_FIRST_THEN_STORE
)

func (rType ReadOffsetType) String() string {
	switch rType {
	case READ_FROM_MEMORY:
		return "READ_FROM_MEMORY"
	case READ_FROM_STORE:
		return "READ_FROM_STORE"
	case MEMORY_FIRST_THEN_STORE:
		return "MEMORY_FIRST_THEN_STORE"
	default:
		return "Unknow"
	}
}

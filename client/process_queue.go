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
package client

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/common/message"
	"github.com/boltmq/common/utils/system"
)

type ProcessQueue struct {
	msgTreeMap        *treeMap
	msgTreeMapMu      sync.RWMutex
	Dropped           bool
	LastPullTimestamp int64
	PullMaxIdleTime   int64
	MsgCount          int64
	QueueOffsetMax    int64
	Consuming         bool
	MsgAccCnt         int64
}

type treeMap struct {
	sync.RWMutex
	keys     []int
	innerMap map[int]*message.MessageExt
}

func newTreeMap() *treeMap {
	return &treeMap{
		innerMap: make(map[int]*message.MessageExt)}
}

func (tmap *treeMap) put(offset int, msg *message.MessageExt) *message.MessageExt {
	old := tmap.innerMap[offset]
	tmap.innerMap[offset] = msg
	tmap.keys = append(tmap.keys, offset)
	sort.Ints(tmap.keys)
	return old
}

func (tmap *treeMap) get(offset int) *message.MessageExt {
	return tmap.innerMap[offset]
}

func (tmap *treeMap) firstKey() int {
	return tmap.keys[0]
}

func (tmap *treeMap) lastKey() int {
	return tmap.keys[len(tmap.keys)-1]
}

func (tmap *treeMap) remove(offset int) *message.MessageExt {
	tmap.Lock()
	defer tmap.Unlock()
	msg := tmap.innerMap[offset]
	newKeys := []int{}
	for _, key := range tmap.keys {
		if key != offset {
			newKeys = append(newKeys, key)
		}
	}
	sort.Ints(newKeys)
	tmap.keys = newKeys
	delete(tmap.innerMap, offset)
	return msg
}

func NewProcessQueue() *ProcessQueue {
	return &ProcessQueue{
		PullMaxIdleTime:   120000,
		LastPullTimestamp: system.CurrentTimeMillis(),
		msgTreeMap:        newTreeMap(),
	}
}

func (pq *ProcessQueue) IsPullExpired() bool {
	return (time.Now().Unix()*1000 - pq.LastPullTimestamp) > pq.PullMaxIdleTime
}

func (pq *ProcessQueue) PutMessage(msgs []*message.MessageExt) bool {
	pq.msgTreeMapMu.Lock()
	defer pq.msgTreeMapMu.Unlock()
	dispatchToConsume := false
	var validMsgCnt int64 = 0
	for _, msg := range msgs {
		old := pq.msgTreeMap.put(int(msg.QueueOffset), msg)
		if old == nil {
			validMsgCnt++
			pq.QueueOffsetMax = msg.QueueOffset
		}

	}
	atomic.AddInt64(&pq.MsgCount, validMsgCnt)
	if len(pq.msgTreeMap.innerMap) > 0 && !pq.Consuming {
		dispatchToConsume = true
		pq.Consuming = true
	}
	if len(msgs) > 0 {
		messageExt := msgs[len(msgs)-1]
		property := messageExt.Properties[message.PROPERTY_MAX_OFFSET]
		if !strings.EqualFold(property, "") {
			maxOffset, _ := strconv.ParseInt(property, 10, 64)
			accTotal := maxOffset - messageExt.QueueOffset
			if accTotal > 0 {
				pq.MsgAccCnt = accTotal
			}
		}
	}
	return dispatchToConsume
}

func (pq *ProcessQueue) RemoveMessage(msgs []*message.MessageExt) int64 {
	pq.msgTreeMapMu.Lock()
	defer pq.msgTreeMapMu.Unlock()
	var result int64 = -1
	if len(pq.msgTreeMap.innerMap) > 0 {
		result = pq.QueueOffsetMax + 1
		var removedCnt int64 = 0
		for _, msg := range msgs {
			prev := pq.msgTreeMap.remove(int(msg.QueueOffset))
			if prev != nil {
				removedCnt--
			}
		}
		atomic.AddInt64(&pq.MsgCount, removedCnt)
		if len(pq.msgTreeMap.innerMap) > 0 {
			result = int64(pq.msgTreeMap.firstKey())
		}
	}

	return result
}

func (pq *ProcessQueue) GetMaxSpan() int64 {
	defer pq.msgTreeMapMu.Unlock()
	pq.msgTreeMapMu.Lock()
	if len(pq.msgTreeMap.innerMap) > 0 {
		return int64(pq.msgTreeMap.lastKey() - pq.msgTreeMap.firstKey())
	}
	return 0
}

func (pq *ProcessQueue) String() string {
	return fmt.Sprintf("[LastPullTimestamp=%v,MsgCount=%v,MsgAccCnt=%v]",
		pq.LastPullTimestamp, pq.MsgCount, pq.MsgAccCnt)
}

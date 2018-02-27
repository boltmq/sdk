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
	"math"
	"strings"
	"sync/atomic"

	"github.com/boltmq/common/message"
)

type TopicPublishInfo struct {
	Order               bool
	HaveTopicRouterInfo bool
	MessageQueues       []*message.MessageQueue
	SendWhichQueue      int64
}

// 取模获取选择队列
func (info *TopicPublishInfo) SelectOneMessageQueue(lastBrokerName string) *message.MessageQueue {
	if lastBrokerName == "" {
		index := info.SendWhichQueue
		atomic.AddInt64(&info.SendWhichQueue, 1)
		value := int(math.Abs(float64(index)))
		pos := value % len(info.MessageQueues)
		mq := info.MessageQueues[pos]
		return mq
	}

	index := info.SendWhichQueue
	atomic.AddInt64(&info.SendWhichQueue, 1)
	for _, mq := range info.MessageQueues {
		index++
		value := int(math.Abs(float64(index)))
		pos := value % len(info.MessageQueues)
		mq = info.MessageQueues[pos]
		if !strings.EqualFold(mq.BrokerName, lastBrokerName) {
			return mq
		}
	}

	return nil
}

func (info *TopicPublishInfo) String() string {
	if info == nil {
		return "<nil>"
	}

	return fmt.Sprintf("order:%t HaveTopicRouterInfo: %t SendWhichQueue: %d",
		info.Order, info.HaveTopicRouterInfo, info.SendWhichQueue)
}

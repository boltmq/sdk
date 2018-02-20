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
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/heartbeat"
)

type consumerInner interface {
	// Set<SubscriptionData>
	Subscriptions() []*heartbeat.SubscriptionData
	// Set<MessageQueue>
	UpdateTopicSubscribeInfo(topic string, info []*message.MessageQueue)
	// 组名称
	GroupName() string
	// 消息类型
	MessageModel() heartbeat.MessageModel
	// 消费类型
	ConsumeType() heartbeat.ConsumeType
	// 消费位置
	ConsumeFromWhere() heartbeat.ConsumeFromWhere
	IsUnitMode() bool
	// 是否需要更新
	IsSubscribeTopicNeedUpdate(topic string) bool
	// 持久化offset
	PersistConsumerOffset()
	// 负载
	DoRebalance()
}

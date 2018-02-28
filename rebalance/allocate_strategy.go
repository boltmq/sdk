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
package rebalance

import "github.com/boltmq/common/message"

// MQAllocateStrategy 消费负载策略接口
type MQAllocateStrategy interface {
	// Allocating by consumer id
	// consumerGroup current consumer group
	// currentCID    current consumer id
	// mqs         message queue set in current topic
	// cids        consumer set in current consumer group
	Allocate(consumerGroup string, currentCID string, mqs []*message.MessageQueue, cids []string) []*message.MessageQueue
	// Algorithm name
	GetName() string
}

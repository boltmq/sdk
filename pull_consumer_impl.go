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
package boltmq

import (
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/sdk/client"
	"github.com/boltmq/sdk/common"
	"github.com/boltmq/sdk/rebalance"
	"github.com/boltmq/sdk/store"
)

type pullConfig struct {
	// Do the same thing for the same Group, the application must be set,and
	// guarantee Globally unique
	consumerGroup string
	// Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
	brokerSuspendMaxTimeMillis int
	// Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not recommended to modify
	consumerTimeoutMillisWhenSuspend int
	consumerPullTimeoutMillis        int                    // The socket timeout in milliseconds
	unitMode                         bool                   // Whether the unit of subscription group
	messageModel                     heartbeat.MessageModel // Consumption pattern,default is clustering
	client                           *client.Config         // the client config
}

// 手动拉取消息
type pullConsumerImpl struct {
	cfg                pullConfig
	registerTopics     []string                     // register topics
	mqAllocateStrategy rebalance.MQAllocateStrategy // Queue allocation algorithm
	offsetStore        store.OffsetStore            // Offset Storage
	mqClient           *client.MQClient
	status             common.SRVStatus
	startTimestamp     int64
	//pullAPIWrapper         *PullAPIWrapper
	//RebalanceImpl          RebalanceImpl
}

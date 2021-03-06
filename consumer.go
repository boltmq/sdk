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
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/sdk/common"
)

const (
	// Founded
	FOUND common.PullStatus = common.FOUND
	// No new message can be pull
	NO_NEW_MSG
	// Filtering results can not match
	NO_MATCHED_MSG
	// Illegal offset，may be too big or too small
	OFFSET_ILLEGAL
)

type PullResult = common.PullResult
type MessageListener interface{}

type PullConsumer interface {
	NameSrvAddrs(addrs []string)
	FetchSubscribeMessageQueues(topic string) []*message.MessageQueue
	Pull(mq *message.MessageQueue, subExpression string, offset int64, maxNums int) (*PullResult, error)
	Start()
	Stop()
}

func NewPullConsumer() PullConsumer {
	return nil
}

type PushConsumer interface {
	NameSrvAddrs(addrs []string)
	SetConsumeFromWhere(consumeFromWhere heartbeat.ConsumeFromWhere)
	SetMessageModel(model heartbeat.MessageModel)
	RegisterMessageListener(listener MessageListener)
	Subscribe(topic string, subExpression string)
	Start()
	Stop()
}

func NewPushConsumer() PushConsumer {
	return nil
}

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
	"github.com/boltmq/sdk/common"
)

type Result = common.Result

type Callback = common.Callback

// 默认Producer
type producerOuter interface {
	ResetClientCfg(cfg Config)
	StartFlag(bool) error
	ShutdownFlag(bool)
	GetCreateTopicKey() string
	GetDefaultTopicQueueNums() int
	producerInner
}

type producerInner interface {
	// 获取生产者topic信息列表
	GetPublishTopicList() []string
	// topic信息是否需要更新
	IsPublishTopicNeedUpdate(topic string) bool
	// 更新topic信息
	UpdateTopicPublishInfo(topic string, info *TopicPublishInfo)
}

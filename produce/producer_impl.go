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
package produce

import (
	"runtime"
	"sync"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/sdk/common"
)

type producerImpl struct {
	producerGroup           string
	cfg                     config
	topicPublishInfoTable   map[string]topicPublishInfo
	topicPublishInfoTableMu sync.RWMutex
	status                  common.SRVStatus
}

func newProducerImpl(producerGroup string) *producerImpl {
	return &producerImpl{
		producerGroup: producerGroup,
		cfg: config{
			createTopic:                      basis.DEFAULT_TOPIC,
			topicQueueNums:                   4,
			sendMsgTimeout:                   3000,
			compressMsgBodyOverHowmuch:       1024 * 4,
			retryTimesWhenSendFailed:         2,
			retryAnotherBrokerWhenNotStoreOK: false,
			maxMessageSize:                   1024 * 128,
			unitMode:                         false,
			client: clientConfig{
				instanceName:                  defaultInstanceName(),
				clientIP:                      defaultLocalAddress(),
				clientCallbackExecutorThreads: runtime.NumCPU(),
				pullNameServerInterval:        1000 * 30,
				heartbeatBrokerInterval:       1000 * 30,
				persistConsumerOffsetInterval: 1000 * 5,
			},
		},
		topicPublishInfoTable: make(map[string]topicPublishInfo),
		status:                common.CREATE_JUST,
	}
}

func (producer *producerImpl) NameSrvAddrs(addrs []string) {
	length := len(addrs)
	if length == 0 {
		return
	}

	nameSrvMap := make(map[string]struct{})
	for _, addr := range addrs {
		nameSrvMap[addr] = struct{}{}
	}
	for _, addr := range producer.cfg.client.nameSrvAddrs {
		nameSrvMap[addr] = struct{}{}
	}
	producer.cfg.client.nameSrvAddrs = nil

	for addr, _ := range nameSrvMap {
		producer.cfg.client.nameSrvAddrs = append(producer.cfg.client.nameSrvAddrs, addr)
	}
}

func (producer *producerImpl) InstanceName(instanceName string) {
	producer.cfg.client.instanceName = instanceName
}

func (producer *producerImpl) Start() error {
	switch producer.status {
	case common.CREATE_JUST:
	case common.RUNNING:
	case common.SHUTDOWN_ALREADY:
	case common.START_FAILED:
	default:
	}

	// 向所有broker发送心跳
	return nil
}

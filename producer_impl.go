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
	"os"
	"runtime"
	"sync"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/sdk/client"
	"github.com/boltmq/sdk/common"
)

type producerImpl struct {
	producerGroup           string
	cfg                     config
	topicPublishInfoTable   map[string]client.TopicPublishInfo
	topicPublishInfoTableMu sync.RWMutex
	status                  common.SRVStatus
}

type config struct {
	createTopic                      string
	topicQueueNums                   int
	sendMsgTimeout                   int64
	compressMsgBodyOverHowmuch       int
	retryTimesWhenSendFailed         int32
	retryAnotherBrokerWhenNotStoreOK bool
	maxMessageSize                   int
	unitMode                         bool
	client                           client.Config
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
			client: client.Config{
				InstanceName:                  defaultInstanceName(),
				ClientIP:                      defaultLocalAddress(),
				ClientCallbackExecutorThreads: runtime.NumCPU(),
				PullNameServerInterval:        1000 * 30,
				HeartbeatBrokerInterval:       1000 * 30,
				PersistConsumerOffsetInterval: 1000 * 5,
			},
		},
		topicPublishInfoTable: make(map[string]client.TopicPublishInfo),
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
	for _, addr := range producer.cfg.client.NameSrvAddrs {
		nameSrvMap[addr] = struct{}{}
	}
	producer.cfg.client.NameSrvAddrs = nil

	for addr, _ := range nameSrvMap {
		producer.cfg.client.NameSrvAddrs = append(producer.cfg.client.NameSrvAddrs, addr)
	}
}

func (producer *producerImpl) InstanceName(instanceName string) {
	producer.cfg.client.InstanceName = instanceName
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

func defaultLocalAddress() string {
	if laddr, err := common.LocalAddress(); err == nil {
		return laddr
	}

	return ""
}

func defaultInstanceName() string {
	instanceName := os.Getenv("BOLTMQ_CLIENT_NAME")
	if instanceName == "" {
		instanceName = "DEFAULT"
	}

	return instanceName
}

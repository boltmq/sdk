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
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/sdk/client"
	"github.com/boltmq/sdk/common"
	"github.com/juju/errors"
)

type producerImpl struct {
	producerGroup           string
	cfg                     config
	topicPublishInfoTable   map[string]client.TopicPublishInfo
	topicPublishInfoTableMu sync.RWMutex
	mqClient                *client.MQClient
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

// Start producer start
func (producer *producerImpl) Start() error {
	return producer.StartFlag(true)
}

// StartFlag
func (producer *producerImpl) StartFlag(flag bool) error {
	switch producer.status {
	case common.CREATE_JUST:
		producer.status = common.START_FAILED
		// 检查配置
		err := producer.checkConfig()
		if err != nil {
			return err
		}

		if producer.producerGroup != basis.CLIENT_INNER_PRODUCER_GROUP {
			producer.cfg.client.ChangeInstanceNameToPID()
		}

		// 初始化MQClientInstance
		producer.mqClient = client.GetAndCreateMQClient(&producer.cfg.client)
		// 注册producer
		producer.mqClient.RegisterProducer(producer.producerGroup, producer)
		// 保存topic信息
		producer.topicPublishInfoTableMu.Lock()
		producer.topicPublishInfoTable[producer.cfg.createTopic] = client.TopicPublishInfo{}
		producer.topicPublishInfoTableMu.Unlock()
		// 启动核心
		if flag {
			producer.mqClient.Start()
		}
		producer.status = common.RUNNING
	case common.RUNNING:
	case common.SHUTDOWN_ALREADY:
	case common.START_FAILED:
	default:
	}

	// 向所有broker发送心跳
	producer.mqClient.SendHeartbeatToAllBrokerWithLock()
	return nil
}

// Stop
func (producer *producerImpl) Stop() {
}

// Send
func (producer *producerImpl) Send(msg *message.Message) (*Result, error) {
	return nil, nil
}

// SendOneWay
func (producer *producerImpl) SendOneWay(msg *message.Message) error {
	return nil
}

// SendCallBack
func (producer *producerImpl) SendCallBack(msg *message.Message, callback Callback) error {
	return nil
}

// 获取topic发布集合
func (producer *producerImpl) GetPublishTopicList() []string {
	var topics []string

	producer.topicPublishInfoTableMu.RLock()
	for k, _ := range producer.topicPublishInfoTable {
		topics = append(topics, k)
	}
	producer.topicPublishInfoTableMu.RUnlock()

	return topics
}

// 是否需要更新topic信息
func (producer *producerImpl) IsPublishTopicNeedUpdate(topic string) bool {
	var isPublish bool

	producer.topicPublishInfoTableMu.RLock()
	if topicInfo, ok := producer.topicPublishInfoTable[topic]; ok {
		isPublish = (len(topicInfo.MessageQueues) == 0)
	} else {
		isPublish = true
	}
	producer.topicPublishInfoTableMu.RUnlock()

	return isPublish
}

// 更新topic信息
func (producer *producerImpl) UpdateTopicPublishInfo(topic string, info *client.TopicPublishInfo) {
	if topic == "" || info == nil {
		logger.Warnf("update topic publish info param invalid, topic: %s info: %s", topic, info)
		return
	}

	producer.topicPublishInfoTableMu.Lock()
	prev, ok := producer.topicPublishInfoTable[topic]
	if ok {
		info.SendWhichQueue = prev.SendWhichQueue
	}
	producer.topicPublishInfoTable[topic] = *info
	producer.topicPublishInfoTableMu.Unlock()

	logger.Infof("update topic publish info prev is %s, new is %s", prev, info)
}

// 检查配置文件
func (producer *producerImpl) checkConfig() error {
	err := common.CheckGroup(producer.producerGroup)
	if err != nil {
		return err
	}

	if producer.producerGroup == basis.DEFAULT_PRODUCER_GROUP {
		return errors.Errorf("producerGroup can not equal %s, please specify another one.",
			basis.DEFAULT_PRODUCER_GROUP)
	}

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

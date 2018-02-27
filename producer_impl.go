// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use producer.cfg file except in compliance with the License.
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/sysflag"
	"github.com/boltmq/common/utils/system"
	"github.com/boltmq/sdk/client"
	"github.com/boltmq/sdk/common"
	"github.com/juju/errors"
)

const (
	defaultTopicQueueNums = 4
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
			topicQueueNums:                   defaultTopicQueueNums,
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
				PullNameServerInteval:         1000 * 30,
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

		// 初始化MQClient
		producer.mqClient = client.GetAndCreateMQClient(&producer.cfg.client)
		//if producer.producerGroup != basis.CLIENT_INNER_PRODUCER_GROUP {
		producer.mqClient.SetDefaultProduer(newProducerImpl(basis.CLIENT_INNER_PRODUCER_GROUP))
		//}

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
	producer.StopFlag(true)
}

// StopFlag
func (producer *producerImpl) StopFlag(flag bool) {
	switch producer.status {
	case common.CREATE_JUST:
	case common.RUNNING:
		producer.status = common.SHUTDOWN_ALREADY
		producer.mqClient.UnRegisterProducer(producer.producerGroup)
		if flag {
			producer.mqClient.Shutdown()
		}
	case common.SHUTDOWN_ALREADY:
	default:
	}
}

// Send 提供同步消息发送方法
func (producer *producerImpl) Send(msg *message.Message) (*Result, error) {
	return producer.sendByTimeout(msg, producer.cfg.sendMsgTimeout)
}

// SendOneWay
func (producer *producerImpl) SendOneWay(msg *message.Message) error {
	_, err := producer.sendMsg(msg, common.ONEWAY, nil, producer.cfg.sendMsgTimeout)
	return err
}

// SendCallBack
func (producer *producerImpl) SendCallBack(msg *message.Message, callback Callback) error {
	_, err := producer.sendMsg(msg, common.ASYNC, callback, producer.cfg.sendMsgTimeout)
	return err
}

// 发送timeout消息
func (producer *producerImpl) sendByTimeout(msg *message.Message, timeout int64) (*Result, error) {
	return producer.sendMsg(msg, common.SYNC, nil, timeout)
}

// 选择需要发送的queue
func (producer *producerImpl) sendMsg(msg *message.Message, commMode common.CommunicationMode,
	callback Callback, timeout int64) (*Result, error) {
	if producer.status != common.RUNNING {
		return nil, errors.Errorf("The producer service state not OK. service status=%s", producer.status)
	}

	err := common.CheckMessage(msg, producer.cfg.maxMessageSize)
	if err != nil {
		return nil, err
	}

	topicPublishInfo := producer.tryToFindTopicPublishInfo(msg.Topic)
	if topicPublishInfo == nil || len(topicPublishInfo.MessageQueues) == 0 {
		return nil, errors.New("send message error topicPublishInfo is nil or messageQueueList length is zero")
	}

	maxTimeout := producer.cfg.sendMsgTimeout + 1000
	timesTotal := 1 + producer.cfg.retryTimesWhenSendFailed
	beginTimestamp := time.Now().Unix() * 1000
	endTimestamp := beginTimestamp

	var (
		times int
		mq    *message.MessageQueue
	)
	for ; times < int(timesTotal) && (endTimestamp-beginTimestamp) < maxTimeout; times++ {
		var lastBrokerName string
		if mq != nil {
			lastBrokerName = mq.BrokerName
		}
		tmpMQ := topicPublishInfo.SelectOneMessageQueue(lastBrokerName)
		if tmpMQ != nil {
			mq = tmpMQ
			sendResult, err := producer.sendKernel(msg, mq, commMode, callback, timeout)
			if err != nil {
				return nil, err
			}

			endTimestamp = system.CurrentTimeMillis()
			switch commMode {
			case common.ASYNC:
				return nil, err
			case common.ONEWAY:
				return nil, err
			case common.SYNC:
				if sendResult != nil && sendResult.Status != SEND_OK && producer.cfg.retryAnotherBrokerWhenNotStoreOK {
					continue
				}
				return sendResult, err
			}
		} else {
			break
		}
	}

	return nil, errors.New("send message error topicPublishInfo failed.")
}

// 查询topic不存在则从nameserver更新
func (producer *producerImpl) tryToFindTopicPublishInfo(topic string) *client.TopicPublishInfo {
	producer.topicPublishInfoTableMu.RLock()
	info, ok := producer.topicPublishInfoTable[topic]
	producer.topicPublishInfoTableMu.RUnlock()
	if !ok {
		return nil
	}

	if len(info.MessageQueues) == 0 {
		info = client.TopicPublishInfo{}
		producer.topicPublishInfoTableMu.Lock()
		producer.topicPublishInfoTable[topic] = info
		producer.topicPublishInfoTableMu.Unlock()

		producer.mqClient.UpdateTopicRouteInfoFromNameServerByTopic(topic)

		producer.topicPublishInfoTableMu.RLock()
		cinfo, ok := producer.topicPublishInfoTable[topic]
		producer.topicPublishInfoTableMu.RUnlock()
		if ok {
			info = cinfo
		}
	}

	if info.HaveTopicRouterInfo && len(info.MessageQueues) != 0 {
		return &info
	}

	producer.mqClient.UpdateTopicRouteInfoFromNameServerByArgs(topic, true, producer)
	producer.topicPublishInfoTableMu.RLock()
	info, ok = producer.topicPublishInfoTable[topic]
	producer.topicPublishInfoTableMu.RUnlock()
	if ok {
		return &info
	}

	return nil
}

// 指定发送到某个queue
func (producer *producerImpl) sendKernel(msg *message.Message, mq *message.MessageQueue,
	commMode common.CommunicationMode, callback Callback, timeout int64) (*Result, error) {
	brokerAddr := producer.mqClient.FindBrokerAddressInPublish(mq.BrokerName)
	if brokerAddr == "" {
		producer.tryToFindTopicPublishInfo(mq.Topic)
		brokerAddr = producer.mqClient.FindBrokerAddressInPublish(mq.BrokerName)
	}

	if brokerAddr == "" {
		errors.Errorf("The broker[%s] not exist ", mq.BrokerName)
	}

	prevBody := msg.Body
	sysFlag := 0
	if producer.tryToCompressMessage(msg) {
		sysFlag |= sysflag.CompressedFlag
	}

	//TODO 事务消息处理
	//TODO 自定义hook处理
	// 构造SendMessageRequestHeader
	header := head.SendMessageRequestHeader{
		ProducerGroup:         producer.producerGroup,
		Topic:                 msg.Topic,
		DefaultTopic:          producer.cfg.createTopic,
		DefaultTopicQueueNums: int32(producer.cfg.topicQueueNums),
		QueueId:               int32(mq.QueueId),
		SysFlag:               int32(sysFlag),
		BornTimestamp:         system.CurrentTimeMillis(),
		Flag:                  int32(msg.Flag),
		Properties:            message.MessageProperties2String(msg.Properties),
		ReconsumeTimes:        0,
		UnitMode:              producer.cfg.unitMode,
	}

	if strings.HasPrefix(header.Topic, basis.RETRY_GROUP_TOPIC_PREFIX) {
		reconsumeTimes := message.GetReconsumeTime(msg)
		if reconsumeTimes != "" {
			times, _ := strconv.Atoi(reconsumeTimes)
			header.ReconsumeTimes = int32(times)
			message.ClearProperty(msg, message.PROPERTY_RECONSUME_TIME)
		}
	}

	sendResult, err := producer.mqClient.SendMessage(brokerAddr, mq.BrokerName, msg, header, timeout, commMode, callback)
	msg.Body = prevBody
	return sendResult, err
}

// 压缩消息体
func (producer *producerImpl) tryToCompressMessage(msg *message.Message) bool {
	if msg == nil || len(msg.Body) == 0 {
		return false
	}

	if len(msg.Body) >= producer.cfg.compressMsgBodyOverHowmuch {
		data := basis.Compress(msg.Body)
		msg.Body = data
		return true
	}

	return false
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

// UpdateTopicPublishInfo 更新topic信息
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

// GetCreateTopicKey
func (producer *producerImpl) GetCreateTopicKey() string {
	return producer.cfg.createTopic
}

// GetDefaultTopicQueueNums
func (producer *producerImpl) GetDefaultTopicQueueNums() int {
	return producer.cfg.topicQueueNums
}

// ResetClientCfg
func (producer *producerImpl) ResetClientCfg(clientCfg client.Config) {
	producer.cfg.client.NameSrvAddrs = clientCfg.NameSrvAddrs
	producer.cfg.client.ClientIP = clientCfg.ClientIP
	producer.cfg.client.InstanceName = clientCfg.InstanceName
	producer.cfg.client.ClientCallbackExecutorThreads = clientCfg.ClientCallbackExecutorThreads
	producer.cfg.client.PullNameServerInteval = clientCfg.PullNameServerInteval
	producer.cfg.client.HeartbeatBrokerInterval = clientCfg.HeartbeatBrokerInterval
	producer.cfg.client.PersistConsumerOffsetInterval = clientCfg.PersistConsumerOffsetInterval
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

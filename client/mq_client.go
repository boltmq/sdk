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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/utils/system"
	"github.com/boltmq/sdk/common"
	"github.com/facebookgo/errgroup"
	"github.com/go-errors/errors"
)

type MQClient struct {
	index             int32
	clientId          string
	cfg               Config
	producerTable     map[string]producerInner        // key: group
	producerTableMu   sync.RWMutex                    //
	consumerTable     map[string]consumerInner        // key: group
	consumerTableMu   sync.RWMutex                    //
	clientAPI         *mqClientAPI                    //
	adminAPI          *mqAdminAPI                     //
	topicRouteTable   map[string]*base.TopicRouteData // key: topic
	topicRouteTableMu sync.RWMutex                    //
	brokerAddrTable   map[string]map[int]string       // key: brokername value: map[key: brokerId value: addr]
	brokerAddrTableMu sync.RWMutex                    //
	pullMsgService    *pullMessageService             //
	rblService        *rebalanceService               //
	defaultProducer   producerOuter                   //
	status            common.SRVStatus                //
	namesrvMu         sync.RWMutex                    //
	heartbeatMu       sync.RWMutex                    //
	timerTaskTable    map[string]*system.Ticker       //
}

// NewMQClient 初始化
func NewMQClient(cfg Config, index int32, clientId string) *MQClient {
	mqClient := &MQClient{
		cfg:             cfg,
		index:           index,
		clientId:        clientId,
		producerTable:   make(map[string]producerInner),
		consumerTable:   make(map[string]consumerInner),
		topicRouteTable: make(map[string]*base.TopicRouteData),
		brokerAddrTable: make(map[string]map[int]string),
		timerTaskTable:  make(map[string]*system.Ticker),
	}

	processor := newRemotingProcessor(mqClient)
	mqClient.clientAPI = newMQClientAPI(processor)
	if mqClient.cfg.NameSrvAddrs != nil && len(mqClient.cfg.NameSrvAddrs) > 0 {
		mqClient.clientAPI.updateNameServerAddressList(mqClient.cfg.NameSrvAddrs)
		logger.Infof("user specified name server address: %v", mqClient.cfg.NameSrvAddrs)
	}

	mqClient.adminAPI = newMQAdminAPI(mqClient)
	mqClient.pullMsgService = newPullMessageService(mqClient)
	mqClient.rblService = newRebalanceService(mqClient)
	//TODO: 消费统计管理器初始化

	return mqClient
}

func (mqClient *MQClient) SetDefaultProduer(producer producerOuter) {
	mqClient.defaultProducer = producer
	mqClient.defaultProducer.ResetClientCfg(mqClient.cfg)
}

// Start
func (mqClient *MQClient) Start() {
	switch mqClient.status {
	case common.CREATE_JUST:
		mqClient.status = common.START_FAILED
		mqClient.clientAPI.start()                // Start request-response channel
		mqClient.startScheduledTasks()            // Start various schedule tasks
		mqClient.pullMsgService.start()           // Start pull service
		mqClient.rblService.start()               // Start rebalance service
		mqClient.defaultProducer.StartFlag(false) // Start push service
		mqClient.status = common.RUNNING          // Set mqClient of status
	case common.RUNNING:
	case common.SHUTDOWN_ALREADY:
	case common.START_FAILED:
	default:
	}
}

// Shutdown
func (mqClient *MQClient) Shutdown() {
	if len(mqClient.consumerTable) > 0 {
		return
	}

	if len(mqClient.producerTable) > 1 {
		return
	}

	switch mqClient.status {
	case common.CREATE_JUST:
	case common.RUNNING:
		mqClient.defaultProducer.StopFlag(false)
		mqClient.status = common.SHUTDOWN_ALREADY
		mqClient.pullMsgService.shutdown()
		for name, timer := range mqClient.timerTaskTable {
			status := timer.Stop()
			logger.Infof("shutdown %s res: %t", name, status)
		}

		mqClient.clientAPI.shutdown()
		mqClient.rblService.shutdown()
		RemoveMQClient(mqClient.clientId)
	case common.SHUTDOWN_ALREADY:
	default:
	}
}

func (mqClient *MQClient) startScheduledTasks() {
	if mqClient.cfg.NameSrvAddrs == nil || len(mqClient.cfg.NameSrvAddrs) == 0 {
		//TODO: namesrv地址为空通过http获取
	}

	// 定时从nameserver更新topic route信息
	updateRouteTicker := system.NewTicker(true, 10*time.Millisecond,
		time.Duration(mqClient.cfg.PullNameServerInteval)*time.Millisecond, func() {
			mqClient.UpdateTopicRouteInfoFromNameServer()
			logger.Infof("update topic routeInfo from name server every [ %d ] sencond.", mqClient.cfg.PullNameServerInteval/1000)
		})
	updateRouteTicker.Start()
	mqClient.timerTaskTable["updateRouteTicker"] = updateRouteTicker

	// 定时清理离线的broker并发送心跳数据
	cleanAndHBTicker := system.NewTicker(true, 1000*time.Millisecond,
		time.Duration(mqClient.cfg.HeartbeatBrokerInterval)*time.Millisecond, func() {
			mqClient.cleanOfflineBroker()
			mqClient.SendHeartbeatToAllBrokerWithLock()
		})
	cleanAndHBTicker.Start()
	mqClient.timerTaskTable["cleanAndHBTicker"] = cleanAndHBTicker

	// 定时持久化consumer的offset
	persistOffsetTicker := system.NewTicker(true, 1000*10*time.Millisecond,
		time.Duration(mqClient.cfg.PersistConsumerOffsetInterval)*time.Millisecond, func() {
			mqClient.persistAllConsumerOffset()
		})
	persistOffsetTicker.Start()
	mqClient.timerTaskTable["persistOffsetTicker"] = persistOffsetTicker

	//TODO: 定时调整线程池的数量
}

// 查找broker的master地址
func (mqClient *MQClient) FindBrokerAddressInPublish(brokerName string) string {
	mqClient.brokerAddrTableMu.RLock()
	baMap, ok := mqClient.brokerAddrTable[brokerName]
	mqClient.brokerAddrTableMu.RUnlock()
	if !ok {
		return ""
	}

	if len(baMap) > 0 {
		return baMap[basis.MASTER_ID]
	}

	return ""
}

func (mqClient *MQClient) UpdateTopicRouteInfoFromNameServerByTopic(topic string) bool {
	return mqClient.UpdateTopicRouteInfoFromNameServerByArgs(topic, false, nil)
}

func (mqClient *MQClient) UpdateTopicRouteInfoFromNameServerByArgs(topic string, isDefault bool, producer producerOuter) bool {
	mqClient.namesrvMu.Lock()
	defer mqClient.namesrvMu.Unlock()

	var topicRouteData *base.TopicRouteData
	if isDefault && producer != nil {
		tmpRouteData, err := mqClient.clientAPI.getDefaultTopicRouteInfoFromNameServer(producer.GetCreateTopicKey(), 3000)
		if err != nil {
			logger.Errorf("update topic routeInfo from name server by args err: %s", err)
			return false
		}

		if tmpRouteData != nil {
			for _, data := range tmpRouteData.QueueDatas {
				queueNums := producer.GetDefaultTopicQueueNums()
				if data.ReadQueueNums < queueNums {
					queueNums = data.ReadQueueNums
				}
				data.ReadQueueNums = queueNums
				data.WriteQueueNums = queueNums
			}
		}

		topicRouteData = tmpRouteData
	} else {
		tmpRouteData, err := mqClient.clientAPI.getTopicRouteInfoFromNameServer(topic, 3000)
		if err != nil {
			logger.Errorf("update topic routeInfo from name server by args err: %s", err)
			return false
		}

		topicRouteData = tmpRouteData
	}

	if topicRouteData == nil {
		logger.Errorf("update topic routeInfo from name server by args err: topic route data is nil.")
		return false
	}

	var changed bool
	if old, ok := mqClient.topicRouteTable[topic]; ok {
		changed = topicRouteDataIsChange(old, topicRouteData)
	} else {
		changed = true
	}

	if !changed {
		changed = mqClient.isNeedUpdateTopicRouteInfo(topic)
		//logger.infof("the topic[%s] route info changed, old[%s] ,new[%s]", topic, old, topicRouteData)
	}

	if !changed {
		return false
	}

	clone := topicRouteData.CloneTopicRouteData()
	mqClient.brokerAddrTableMu.Lock()
	for _, data := range clone.BrokerDatas {
		mqClient.brokerAddrTable[data.BrokerName] = data.BrokerAddrs
	}
	mqClient.brokerAddrTableMu.Unlock()

	// update pub info
	publishInfo := mqClient.topicRouteData2TopicPublishInfo(topic, topicRouteData)
	publishInfo.HaveTopicRouterInfo = true

	var producers []producerInner
	mqClient.producerTableMu.RLock()
	for _, pi := range mqClient.producerTable {
		if pi != nil {
			producers = append(producers, pi)
		}
	}
	mqClient.producerTableMu.RUnlock()

	for _, pi := range producers {
		pi.UpdateTopicPublishInfo(topic, publishInfo)
	}

	// update sub info
	var consumers []consumerInner
	subscribeInfo := mqClient.topicRouteData2TopicSubscribeInfo(topic, topicRouteData)
	mqClient.consumerTableMu.RLock()
	for _, ci := range mqClient.consumerTable {
		if ci != nil {
			consumers = append(consumers, ci)
		}
	}
	mqClient.consumerTableMu.RUnlock()

	for _, ci := range consumers {
		ci.UpdateTopicSubscribeInfo(topic, subscribeInfo)
	}

	mqClient.topicRouteTableMu.Lock()
	mqClient.topicRouteTable[topic] = clone
	mqClient.topicRouteTableMu.Unlock()
	logger.Infof("put topic route data to table, topic[%s]", topic)

	return true
}

// 是否需要更新topic路由信息
func (mqClient *MQClient) isNeedUpdateTopicRouteInfo(topic string) bool {
	var result bool

	mqClient.producerTableMu.RLock()
	for _, pi := range mqClient.producerTable {
		if pi != nil {
			result = pi.IsPublishTopicNeedUpdate(topic)
		}
	}
	mqClient.producerTableMu.RUnlock()

	mqClient.producerTableMu.RLock()
	for _, ci := range mqClient.consumerTable {
		if ci != nil {
			result = ci.IsSubscribeTopicNeedUpdate(topic)
		}
	}
	mqClient.producerTableMu.RUnlock()

	return result
}

// topicRouteData2TopicPublishInfo 路由信息转发布信息
func (mqClient *MQClient) topicRouteData2TopicPublishInfo(topic string, topicRouteData *base.TopicRouteData) *TopicPublishInfo {
	info := &TopicPublishInfo{}

	if topicRouteData.OrderTopicConf != "" {
		brokers := strings.Split(topicRouteData.OrderTopicConf, ";")
		for _, broker := range brokers {
			items := strings.Split(broker, ":")
			nums, err := strconv.Atoi(items[1])
			if err != nil {
				return info
			}

			for i := 0; i < nums; i++ {
				info.MessageQueues = append(info.MessageQueues,
					&message.MessageQueue{Topic: topic, BrokerName: items[0], QueueId: i})
			}
		}
		info.Order = true
		return info
	}

	qds := base.QueueDatas(topicRouteData.QueueDatas)
	sort.Sort(qds)

	for _, queueData := range qds {
		if constant.IsWriteable(queueData.Perm) {
			for _, bd := range topicRouteData.BrokerDatas {
				if queueData.BrokerName == bd.BrokerName {
					if _, ok := bd.BrokerAddrs[basis.MASTER_ID]; ok {
						for i := 0; i < int(queueData.WriteQueueNums); i++ {
							info.MessageQueues = append(info.MessageQueues,
								&message.MessageQueue{Topic: topic, BrokerName: bd.BrokerName, QueueId: i})
						}
					}
					break
				}
			}
		}
	}
	info.Order = false

	return info
}

// 路由信息转订阅信息
func (mqClient *MQClient) topicRouteData2TopicSubscribeInfo(topic string, topicRouteData *base.TopicRouteData) []*message.MessageQueue {
	//TODO: 去重
	mqs := []*message.MessageQueue{}
	for _, qd := range topicRouteData.QueueDatas {
		if constant.IsReadable(qd.Perm) {
			for i := 0; i < qd.ReadQueueNums; i++ {
				mq := &message.MessageQueue{Topic: topic, BrokerName: qd.BrokerName, QueueId: i}
				mqs = append(mqs, mq)
			}
		}
	}

	return mqs
}

// 立即执行负载
func (mqClient *MQClient) rebalanceImmediately() {
	mqClient.rblService.wakeup <- struct{}{}
}

func (mqClient *MQClient) ConsumeMessageDirectly(msg *message.MessageExt,
	consumerGroup, brokerName string) *head.ConsumeMessageDirectlyResult {
	mqClient.consumerTableMu.RLock()
	ci, ok := mqClient.consumerTable[consumerGroup]
	mqClient.consumerTableMu.RUnlock()
	if !ok {
		return nil
	}

	ci = ci
	//TODO:
	//result := ci.consumeMessageService.ConsumeMessageDirectly(msg, brokerName)

	//if consumer, ok := mqConsumerInner.(*DefaultMQPushConsumerImpl); ok && consumer != nil {
	//result := consumer.consumeMessageService.ConsumeMessageDirectly(msg, brokerName)
	//return result
	//}
	return nil
}

func (mqClient *MQClient) doRebalance() {
	mqClient.consumerTableMu.RLock()
	for _, ci := range mqClient.consumerTable {
		if ci != nil {
			ci.DoRebalance()
		}
	}
	mqClient.consumerTableMu.RUnlock()
}

func (mqClient *MQClient) selectConsumer(group string) consumerInner {
	mqClient.consumerTableMu.RLock()
	defer mqClient.consumerTableMu.RUnlock()

	ci, ok := mqClient.consumerTable[group]
	if !ok {
		return nil
	}

	return ci
}

// 从nameserver更新路由信息
func (mqClient *MQClient) UpdateTopicRouteInfoFromNameServer() {
	var consumers []consumerInner
	mqClient.consumerTableMu.RLock()
	for _, ci := range mqClient.consumerTable {
		consumers = append(consumers, ci)
	}
	mqClient.consumerTableMu.RUnlock()

	for _, ci := range consumers {
		subscriptions := ci.Subscriptions()
		for _, subData := range subscriptions {
			mqClient.UpdateTopicRouteInfoFromNameServerByTopic(subData.Topic)
		}
	}

	var producers []producerInner
	mqClient.producerTableMu.RLock()
	for _, pi := range mqClient.producerTable {
		producers = append(producers, pi)
	}
	mqClient.producerTableMu.RUnlock()

	for _, pi := range producers {
		topics := pi.GetPublishTopicList()
		for _, topic := range topics {
			mqClient.UpdateTopicRouteInfoFromNameServerByTopic(topic)
		}
	}
}

// Remove offline broker
func (mqClient *MQClient) cleanOfflineBroker() {
	brokerAddrTable := make(map[string]map[int]string)

	mqClient.namesrvMu.Lock()
	mqClient.brokerAddrTableMu.RLock()
	for brokerName, baMap := range mqClient.brokerAddrTable {
		clone := make(map[int]string)
		for bid, addr := range baMap {
			clone[bid] = addr
		}
		brokerAddrTable[brokerName] = clone
	}
	mqClient.brokerAddrTableMu.RUnlock()

	for brokerName, baMap := range brokerAddrTable {
		for bid, addr := range baMap {
			if !mqClient.isBrokerAddrExistInTopicRouteTable(addr) {
				delete(baMap, bid)
				logger.Infof("the broker[%s] addr[%s] is offline, remove it", brokerName, addr)
			}
		}

		if len(baMap) == 0 {
			mqClient.brokerAddrTableMu.Lock()
			delete(mqClient.brokerAddrTable, brokerName)
			mqClient.brokerAddrTableMu.Unlock()
		} else {
			mqClient.brokerAddrTableMu.Lock()
			mqClient.brokerAddrTable[brokerName] = baMap
			mqClient.brokerAddrTableMu.Unlock()
		}
	}

	mqClient.namesrvMu.Unlock()
}

// 判断brokder地址在路由表中是否存在
func (mqClient *MQClient) isBrokerAddrExistInTopicRouteTable(addr string) bool {
	mqClient.topicRouteTableMu.RLock()
	defer mqClient.topicRouteTableMu.RUnlock()
	for _, routeData := range mqClient.topicRouteTable {
		for _, brokerData := range routeData.BrokerDatas {
			for _, brokerAddr := range brokerData.BrokerAddrs {
				if brokerAddr == addr {
					return true
				}
			}
		}
	}

	return false
}

// 向所有boker发送心跳
func (mqClient *MQClient) SendHeartbeatToAllBrokerWithLock() error {
	mqClient.heartbeatMu.Lock()
	defer mqClient.heartbeatMu.Unlock()

	return mqClient.sendHeartbeatToAllBroker()
	//todo uploadFilterClassSource
}

// 向所有boker发送心跳
func (mqClient *MQClient) sendHeartbeatToAllBroker() error {
	heartbeatData := mqClient.prepareHeartbeatData()

	if len(heartbeatData.ProducerDatas) == 0 &&
		len(heartbeatData.ConsumerDatas) == 0 {
		return errors.Errorf("sending hearbeat, but no consumer and no producer")
	}

	brokerAddrTable := make(map[string]string)
	mqClient.brokerAddrTableMu.RLock()
	for brokerName, brokerData := range mqClient.brokerAddrTable {
		for bid, addr := range brokerData {
			if addr == "" {
				continue
			}

			if len(heartbeatData.ConsumerDatas) == 0 && bid != basis.MASTER_ID {
				continue
			}

			brokerAddrTable[addr] = brokerName
		}
	}
	mqClient.brokerAddrTableMu.RUnlock()

	// send msg to broker
	var g errgroup.Group
	for addr, brokerName := range brokerAddrTable {
		err := mqClient.clientAPI.sendHeartbeat(addr, heartbeatData, 3000)
		if err != nil {
			g.Error(errors.Errorf("send heartbeat to broker[%s, %s] fail, err: %s", brokerName, addr, err))
		} else {
			logger.Infof("send heartbeat to broker[%s, %s] success.", brokerName, addr)
		}
	}

	return g.Wait()
}

// 准备心跳数据
func (mqClient *MQClient) prepareHeartbeatData() *heartbeat.HeartbeatData {
	heartbeatData := &heartbeat.HeartbeatData{
		ClientID: mqClient.clientId,
	}

	mqClient.producerTableMu.RLock()
	for k, v := range mqClient.producerTable {
		if v != nil {
			producerData := heartbeat.ProducerData{GroupName: k}
			heartbeatData.ProducerDatas = append(heartbeatData.ProducerDatas, producerData)
		}
	}
	mqClient.producerTableMu.RUnlock()

	mqClient.consumerTableMu.RLock()
	for _, v := range mqClient.consumerTable {
		if v != nil {
			consumerData := heartbeat.ConsumerData{
				GroupName:        v.GroupName(),
				ConsumeType:      v.ConsumeType(),
				ConsumeFromWhere: v.ConsumeFromWhere(),
				MessageModel:     v.MessageModel(),
				UnitMode:         v.IsUnitMode(),
			}

			for _, subData := range v.Subscriptions() {
				consumerData.SubscriptionDatas = append(consumerData.SubscriptionDatas, *subData)
			}

			heartbeatData.ConsumerDatas = append(heartbeatData.ConsumerDatas, consumerData)
		}
	}
	mqClient.consumerTableMu.RUnlock()

	return heartbeatData
}

// 持久化所有consumer的offset
func (mqClient *MQClient) persistAllConsumerOffset() {
	var consumers []consumerInner

	mqClient.consumerTableMu.RLock()
	for _, ci := range mqClient.consumerTable {
		consumers = append(consumers, ci)
	}
	mqClient.consumerTableMu.RUnlock()

	for _, ci := range consumers {
		ci.PersistConsumerOffset()
	}
}

// topic路由信息是否改变
func topicRouteDataIsChange(old *base.TopicRouteData, new *base.TopicRouteData) bool {
	if old == nil || new == nil {
		return true
	}

	nold := old.CloneTopicRouteData()
	nnew := new.CloneTopicRouteData()

	oldBrokerDatas := base.BrokerDatas(nold.BrokerDatas)
	newBrokerDatas := base.BrokerDatas(nnew.BrokerDatas)
	oldQueueDatas := base.QueueDatas(nold.QueueDatas)
	newQueueDatas := base.QueueDatas(nnew.QueueDatas)

	sort.Sort(oldBrokerDatas)
	sort.Sort(newBrokerDatas)
	sort.Sort(oldQueueDatas)
	sort.Sort(newQueueDatas)

	return !nold.Equals(nnew)
}

// RegisterProducer 将生产者group和发送类保存到内存中
func (mqClient *MQClient) RegisterProducer(group string, producer producerInner) bool {
	var flag bool

	mqClient.producerTableMu.Lock()
	if _, ok := mqClient.producerTable[group]; !ok {
		mqClient.producerTable[group] = producer
		flag = true
	}
	mqClient.producerTableMu.Unlock()

	return flag
}

// UnRegisterProducer 注销生产者
func (mqClient *MQClient) UnRegisterProducer(group string) {
	mqClient.producerTableMu.Lock()
	delete(mqClient.producerTable, group)
	mqClient.producerTableMu.Unlock()

	mqClient.unRegisterClientWithLock(group, "")
}

// RegisterConsumer 将生产者group和发送类保存到内存中
func (mqClient *MQClient) RegisterConsumer(group string, consumer consumerInner) bool {
	var flag bool

	mqClient.consumerTableMu.Lock()
	if _, ok := mqClient.consumerTable[group]; !ok {
		mqClient.consumerTable[group] = consumer
		flag = true
	}
	mqClient.consumerTableMu.Unlock()

	return flag
}

// UnRegisterConsumer 注销消费者
func (mqClient *MQClient) UnRegisterConsumer(group string) {
	mqClient.consumerTableMu.Lock()
	delete(mqClient.consumerTable, group)
	mqClient.consumerTableMu.Unlock()

	mqClient.unRegisterClientWithLock("", group)
}

// 注销客户端
func (mqClient *MQClient) unRegisterClientWithLock(producerGroup, consumerGroup string) {
	mqClient.heartbeatMu.Lock()
	defer mqClient.heartbeatMu.Unlock()

	brokerAddrTable := make(map[string]string)
	mqClient.brokerAddrTableMu.RLock()
	for brokerName, brokerData := range mqClient.brokerAddrTable {
		if brokerData == nil {
			continue
		}

		for _, addr := range brokerData {
			if addr != "" {
				brokerAddrTable[addr] = brokerName
			}
		}
	}
	mqClient.brokerAddrTableMu.RUnlock()

	// unregister client
	for addr, brokerName := range brokerAddrTable {
		err := mqClient.clientAPI.unRegisterClient(addr, mqClient.clientId, producerGroup, consumerGroup, 3000)
		if err != nil {
			logger.Infof("unregister client [producerGroup: %s, consumerGroup: %s] from broker[%s, %s] failed: %s",
				producerGroup, consumerGroup, brokerName, addr, err)
		} else {
			logger.Infof("unregister client [producerGroup: %s, consumerGroup: %s] from broker[%s, %s] success",
				producerGroup, consumerGroup, brokerName, addr)
		}
	}
}

func (mqClient *MQClient) findConsumerIdList(topic string, group string) ([]string, error) {
	brokerAddr := mqClient.findBrokerAddrByTopic(topic)
	if brokerAddr == "" {
		mqClient.UpdateTopicRouteInfoFromNameServerByTopic(topic)
		brokerAddr = mqClient.findBrokerAddrByTopic(topic)
	}

	if brokerAddr != "" {
		return mqClient.clientAPI.getConsumerIdListByGroup(brokerAddr, group, 3000)
	}

	return []string{}, nil
}

func (mqClient *MQClient) findBrokerAddrByTopic(topic string) string {
	mqClient.topicRouteTableMu.RLock()
	defer mqClient.topicRouteTableMu.RUnlock()

	if topicRouteData, ok := mqClient.topicRouteTable[topic]; ok {
		if topicRouteData != nil {
			if len(topicRouteData.BrokerDatas) > 0 {
				bd := topicRouteData.BrokerDatas[0]
				return bd.SelectBrokerAddr()
			}
		}
	}

	return ""
}

func (mqClient *MQClient) findBrokerAddressInAdmin(brokerName string) (*common.FindBrokerResult, error) {
	mqClient.brokerAddrTableMu.RLock()
	brokerMap, ok := mqClient.brokerAddrTable[brokerName]
	mqClient.brokerAddrTableMu.RUnlock()
	if !ok {
		return nil, errors.Errorf("not found broker addr by %s", brokerName)
	}

	if brokerMap == nil || len(brokerMap) == 0 {
		return nil, errors.Errorf("not found broker addr by %s", brokerName)
	}

	var result *common.FindBrokerResult
	for bid, addr := range brokerMap {
		if addr == "" {
			continue
		}

		result = &common.FindBrokerResult{
			BrokerAddr: addr,
		}
		if bid != basis.MASTER_ID {
			result.Slave = true
		}
		break
	}

	if result == nil {
		return nil, errors.Errorf("not found broker addr by %s", brokerName)
	}

	return result, nil
}

func (mqClient *MQClient) findBrokerAddressInSubscribe(brokerName string, brokerId int,
	onlyThisBroker bool) (*common.FindBrokerResult, error) {
	mqClient.brokerAddrTableMu.RLock()
	brokerMap, ok := mqClient.brokerAddrTable[brokerName]
	mqClient.brokerAddrTableMu.RUnlock()
	if !ok {
		return nil, errors.Errorf("not found broker addr by %s,%d", brokerName, brokerId)
	}

	if brokerMap == nil || len(brokerMap) == 0 || brokerId >= len(brokerMap) {
		return nil, errors.Errorf("not found broker addr by %s,%d", brokerName, brokerId)
	}

	baddr := brokerMap[brokerId]
	slave := (brokerId != basis.MASTER_ID)
	if baddr == "" && !onlyThisBroker {
		for bid, addr := range brokerMap {
			if addr == "" {
				continue
			}

			baddr = addr
			slave = (bid != basis.MASTER_ID)
			break
		}
	}

	if baddr == "" {
		return nil, errors.Errorf("not found broker addr by %s,%d", brokerName, brokerId)
	}

	return &common.FindBrokerResult{
		BrokerAddr: baddr,
		Slave:      slave,
	}, nil
}

func (mqClient *MQClient) SendMessage(addr string, brokerName string, msg *message.Message, header head.SendMessageRequestHeader,
	timeout int64, commMode common.CommunicationMode, callback Callback) (*Result, error) {
	return mqClient.clientAPI.sendMessage(addr, brokerName, msg, header, timeout, commMode, callback)
}

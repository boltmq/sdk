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
	"sync"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/sdk/common"
)

type MQClient struct {
	index             int32
	clientId          string
	cfg               Config
	producers         map[string]producerInner        // key: group
	producersMu       sync.RWMutex                    //
	consumers         map[string]consumerInner        // key: group
	consumersMu       sync.RWMutex                    //
	clientAPI         *mqClientAPI                    //
	adminAPI          *mqAdminAPI                     //
	topicRouteTable   map[string]*base.TopicRouteData // key: topic
	topicRouteTableMu sync.RWMutex                    //
	brokerAddrTable   map[string]map[int]string       // key: brokername value: map[key: brokerId value: addr]
	brokerAddrTableMu sync.RWMutex                    //
	rblService        *rebalanceService               //
	status            common.SRVStatus                //
	namesrvMu         sync.RWMutex                    //
	heartbeatMu       sync.RWMutex                    //
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

func (mqClient *MQClient) UpdateTopicRouteInfoFromNameServerByArgs(topic string, isDefault bool, producer interface{}) bool {
	//TODO:
	return false
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
	//TODO:
	//mqClient.rebalanceService.Wakeup <- true
}

func (mqClient *MQClient) ConsumeMessageDirectly(msg *message.MessageExt,
	consumerGroup, brokerName string) *head.ConsumeMessageDirectlyResult {
	mqClient.consumersMu.RLock()
	ci, ok := mqClient.consumers[consumerGroup]
	mqClient.consumersMu.RUnlock()
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
	mqClient.consumersMu.RLock()
	for _, ci := range mqClient.consumers {
		if ci != nil {
			ci.DoRebalance()
		}
	}
	mqClient.consumersMu.RUnlock()
}

func (mqClient *MQClient) selectConsumer(group string) consumerInner {
	mqClient.consumersMu.RLock()
	ci, ok := mqClient.consumers[group]
	mqClient.consumersMu.RUnlock()
	if !ok {
		return nil
	}

	return ci
}

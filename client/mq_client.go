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
)

type MQClient struct {
	index           int32
	clientId        string
	cfg             Config
	producers       map[string]producerInner        // key: group
	producersMu     sync.RWMutex                    //
	consumers       map[string]consumerInner        // key: group
	consumersMu     sync.RWMutex                    //
	clientAPI       *mqClientAPI                    //
	adminAPI        *mqAdminAPI                     //
	topicRouteTable map[string]*base.TopicRouteData // key: topic
	brokerAddrTable map[string]map[int]string       // key: brokername value: map[key: brokerId value: addr]
	namesrvMu       sync.RWMutex                    //
	heartbeatMu     sync.RWMutex                    //
}

// 查找broker的master地址
func (mqClient *MQClient) FindBrokerAddressInPublish(brokerName string) string {
	baMap, ok := mqClient.brokerAddrTable[brokerName]
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

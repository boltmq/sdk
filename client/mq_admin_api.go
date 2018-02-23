// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or apiied.
// See the License for the specific language governing permissions and
// limitations under the License.
package client

import (
	"sort"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/base"
	"github.com/facebookgo/errgroup"
	"github.com/go-errors/errors"
)

type mqAdminAPI struct {
	mqClient *MQClient
}

func newMQAdminAPI(mqClient *MQClient) *mqAdminAPI {
	return &mqAdminAPI{mqClient: mqClient}
}

func (api *mqAdminAPI) maxOffset(mq *message.MessageQueue) (int64, error) {
	brokerAddr := api.mqClient.FindBrokerAddressInPublish(mq.BrokerName)
	if brokerAddr == "" {
		api.mqClient.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		brokerAddr = api.mqClient.FindBrokerAddressInPublish(mq.BrokerName)
	}

	if brokerAddr == "" {
		return api.mqClient.clientAPI.getMaxOffset(brokerAddr, mq.Topic, mq.QueueId, 1000*3)
	}

	return 0, errors.Errorf("the broker[%s] not exist", mq.BrokerName)
}

func (api *mqAdminAPI) createTopic(key, newTopic string, queueNum int32, topicSysFlag int) error {
	topicRouteData, err := api.mqClient.clientAPI.getTopicRouteInfoFromNameServer(key, 1000*3)
	if err != nil {
		return err
	}

	if topicRouteData == nil {
		return errors.Errorf("create topic route date is nil. key=%s, newTopic=%s", key, newTopic)
	}

	brokers := topicRouteData.BrokerDatas
	if brokers == nil || len(brokers) == 0 {
		return errors.Errorf("create topic failed, not found broker.")
	}

	// 类型转换
	sortBrokers := base.BrokerDatas(brokers)
	sort.Sort(sortBrokers)
	var g errgroup.Group
	for _, brokerData := range sortBrokers {
		brokerAddr := brokerData.BrokerAddrs[basis.MASTER_ID]
		if brokerAddr != "" {
			topicConfig := &base.TopicConfig{
				TopicName:      newTopic,
				ReadQueueNums:  queueNum,
				WriteQueueNums: queueNum,
				Perm:           constant.PERM_READ | constant.PERM_WRITE,
				TopicSysFlag:   topicSysFlag,
			}

			err = api.mqClient.clientAPI.createTopic(brokerAddr, key, topicConfig, 1000*3)
			if err != nil {
				g.Error(err)
			}
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (api *mqAdminAPI) fetchSubscribeMessageQueues(topic string) ([]*message.MessageQueue, error) {
	routeData, err := api.mqClient.clientAPI.getTopicRouteInfoFromNameServer(topic, 1000*3)
	if err != nil {
		return nil, err
	}

	if routeData == nil {
		return []*message.MessageQueue{}, nil
	}

	return api.mqClient.topicRouteData2TopicSubscribeInfo(topic, routeData), nil
}

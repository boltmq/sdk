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
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/net/remoting"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/protocol/namesrv"
	"github.com/boltmq/sdk/common"
	"github.com/facebookgo/errgroup"
	"github.com/go-errors/errors"
)

type mqClientAPI struct {
	groupPrefix    string
	clientIP       string
	remotingClient remoting.RemotingClient
	processor      *remotingProcessor
}

// Start 调用romoting的start
func (api *mqClientAPI) start() {
	api.remotingClient.Start()
	value, err := api.getProjectGroupByIp(api.clientIP, 3000)
	if err != nil && value != "" {
		api.groupPrefix = value
	}
}

// 关闭
func (api *mqClientAPI) shutdown() {
	api.remotingClient.Shutdown()
}

func (api *mqClientAPI) getProjectGroupByIp(ip string, timeout int64) (string, error) {
	return api.getKVConfigValue(namesrv.NAMESPACE_PROJECT_CONFIG, ip, timeout)
}

// 发送心跳到broker
func (api *mqClientAPI) sendHeartbeat(addr string, heartbeatData *heartbeat.HeartbeatData, timeout int64) error {
	if api.groupPrefix != "" {
		for _, cData := range heartbeatData.ConsumerDatas {
			cData.GroupName = common.BuildWithProjectGroup(cData.GroupName, api.groupPrefix)
			for _, subData := range cData.SubscriptionDatas {
				subData.Topic = common.BuildWithProjectGroup(subData.Topic, api.groupPrefix)
			}
		}

		for _, pData := range heartbeatData.ProducerDatas {
			pData.GroupName = common.BuildWithProjectGroup(pData.GroupName, api.groupPrefix)
		}
	}

	request := protocol.CreateRequestCommand(protocol.HEART_BEAT)
	request.Body = heartbeatData.Encode()
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return errors.Errorf("send heartbeat to broker err: %s", err)
	}

	if response == nil {
		return errors.Errorf("send heartbeat to broker err: response is nil")
	}

	if response.Code != protocol.SUCCESS {
		return errors.Errorf("send heartbeat to broker failed, code:%d, remark:%s.", response.Code, response.Remark)
	}

	return nil
}

// 获取Topic路由信息
func (api *mqClientAPI) getDefaultTopicRouteInfoFromNameServer(topic string, timeout int64) (*base.TopicRouteData, error) {
	header := head.GetRouteInfoRequestHeader{Topic: topic}
	request := protocol.CreateRequestCommand(protocol.GET_ROUTEINTO_BY_TOPIC, &header)
	response, err := api.remotingClient.InvokeSync("", request, timeout)
	if err != nil {
		return nil, errors.Errorf("get default topic routeInfo from name server err: %s", err)
	}

	if response == nil {
		return nil, errors.Errorf("get default topic routeInfo from name server err: response is nil")
	}

	switch response.Code {
	case protocol.TOPIC_NOT_EXIST:
		return nil, errors.Errorf("get default topic routeInfo from name server err: topic[%s] is not exist value", topic)
	case protocol.SUCCESS:
		body := response.Body
		if body == nil || len(body) == 0 {
			return nil, errors.Errorf("get default topic routeInfo from name server body is empty.")
		}

		topicRouteData := &base.TopicRouteData{}
		err := topicRouteData.Decode(body)
		if err != nil {
			return nil, errors.Errorf("get default topic routeInfo from name server decode err: %s.", err)
		}

		return topicRouteData, nil
	}

	return nil, errors.Errorf("get default topic routeInfo from name server failed, code:%d, remark:%s.", response.Code, response.Remark)
}

func (api *mqClientAPI) getTopicRouteInfoFromNameServer(topic string, timeout int64) (*base.TopicRouteData, error) {
	if api.groupPrefix != "" {
		topic = common.BuildWithProjectGroup(topic, api.groupPrefix)
	}

	header := &head.GetRouteInfoRequestHeader{Topic: topic}
	request := protocol.CreateRequestCommand(protocol.GET_ROUTEINTO_BY_TOPIC, header)
	response, err := api.remotingClient.InvokeSync("", request, timeout)
	if err != nil {
		return nil, errors.Errorf("get topic routeInfo from name server err: %s", err)
	}

	if response == nil {
		return nil, errors.Errorf("get topic routeInfo from name server err: response is nil")
	}

	switch response.Code {
	case protocol.TOPIC_NOT_EXIST:
		return nil, errors.Errorf("get topic routeInfo from name server err: topic[%s] is not exist value", topic)
	case protocol.SUCCESS:
		body := response.Body
		if body == nil || len(body) == 0 {
			return nil, errors.Errorf("get topic routeInfo from name server body is empty.")
		}

		topicRouteData := &base.TopicRouteData{}
		err := topicRouteData.Decode(body)
		if err != nil {
			return nil, errors.Errorf("get default topic routeInfo from name server decode err: %s.", err)
		}

		return topicRouteData, nil
	}

	return nil, errors.Errorf("get topic routeInfo from name server failed, code:%d, remark:%s.", response.Code, response.Remark)
}

func (api *mqClientAPI) sendMessage(addr string, brokerName string, msg *message.Message, header head.SendMessageRequestHeader,
	timeout int64, communicationMode common.CommunicationMode, callback Callback) (*Result, error) {
	if api.groupPrefix != "" {
		msg.Topic = common.BuildWithProjectGroup(msg.Topic, api.groupPrefix)
		header.ProducerGroup = common.BuildWithProjectGroup(header.ProducerGroup, api.groupPrefix)
		header.Topic = common.BuildWithProjectGroup(header.Topic, api.groupPrefix)
	}

	// 默认send采用v2版本
	headerV2 := head.CreateSendMessageRequestHeaderV2(&header)
	request := protocol.CreateRequestCommand(protocol.SEND_MESSAGE_V2, headerV2)
	request.Body = msg.Body

	switch communicationMode {
	case common.ONEWAY:
		request.MarkOnewayRPC()
		err := api.remotingClient.InvokeOneway(addr, request, timeout)
		return nil, err
	case common.ASYNC:
		api.remotingClient.InvokeAsync(addr, request, timeout, func(responseFuture *remoting.ResponseFuture) {
			if callback == nil {
				return
			}

			response := responseFuture.GetRemotingCommand()
			sendResult, err := api.processSendResponse(brokerName, msg, response)
			callback(sendResult, err)
		})
		return nil, nil
	case common.SYNC:
		response, err := api.remotingClient.InvokeSync(addr, request, timeout)
		if err != nil {
			return nil, errors.Errorf("send message err:%s.", err)
		}

		return api.processSendResponse(brokerName, msg, response)
	default:
	}

	return nil, errors.Errorf("send message communication mode:%s is not exsit.", communicationMode)
}

// 处理发送消息响应
func (api *mqClientAPI) processSendResponse(brokerName string, msg *message.Message, response *protocol.RemotingCommand) (*Result, error) {
	if response == nil {
		return nil, errors.Errorf("process send response err: response is nil.")
	}

	var status common.ResultStatus
	switch response.Code {
	case protocol.FLUSH_DISK_TIMEOUT:
		status = common.FLUSH_DISK_TIMEOUT
	case protocol.FLUSH_SLAVE_TIMEOUT:
		status = common.FLUSH_SLAVE_TIMEOUT
	case protocol.SLAVE_NOT_AVAILABLE:
		status = common.SLAVE_NOT_AVAILABLE
	case protocol.SUCCESS:
		status = common.SEND_OK
	default:
		status = common.SEND_UNKNOW
	}

	if status == common.SEND_UNKNOW {
		return nil, errors.Errorf("process send response err, code:%d remark:%s.", response.Code, response.Remark)
	}

	respHeader := &head.SendMessageResponseHeader{}
	response.DecodeCommandCustomHeader(respHeader)
	messageQueue := &message.MessageQueue{Topic: msg.Topic, BrokerName: brokerName, QueueId: int(respHeader.QueueId)}
	sendResult := common.NewResult(api.groupPrefix, status, respHeader.MsgId, messageQueue, respHeader.QueueOffset, respHeader.TransactionId)

	return sendResult, nil
}

func (api *mqClientAPI) pullMessage(addr string, header head.PullMessageRequestHeader,
	timeout int64, communicationMode common.CommunicationMode, pullCallback PullCallback) (*PullResultExt, error) {
	if api.groupPrefix != "" {
		header.ConsumerGroup = common.BuildWithProjectGroup(header.ConsumerGroup, api.groupPrefix)
		header.Topic = common.BuildWithProjectGroup(header.Topic, api.groupPrefix)
	}

	request := protocol.CreateRequestCommand(protocol.PULL_MESSAGE, &header)
	switch communicationMode {
	case common.ONEWAY:
	case common.SYNC:
		return api.pullMessageSync(addr, request, timeout)
	case common.ASYNC:
		api.pullMessageAsync(addr, request, timeout, pullCallback)
	default:
	}

	return nil, errors.Errorf("pull message communication mode:%d not support.", communicationMode)
}

func (api *mqClientAPI) pullMessageSync(addr string, request *protocol.RemotingCommand, timeout int64) (*PullResultExt, error) {
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return nil, errors.Errorf("pull message sync err: %s.", err)
	}

	if response == nil {
		return nil, errors.Errorf("pull message err: response is nil.")
	}

	return api.processPullResponse(response), nil
}

func (api *mqClientAPI) pullMessageAsync(addr string, request *protocol.RemotingCommand, timeout int64, pullCallback PullCallback) {
	invokeCallback := func(responseFuture *remoting.ResponseFuture) {
		response := responseFuture.GetRemotingCommand()
		if response != nil {
			pullResultExt := api.processPullResponse(response)
			pullCallback.OnSuccess(pullResultExt)
		} else {
			if !responseFuture.IsSendRequestOK() {
				logger.Warnf("send request not ok")
			} else if responseFuture.IsTimeout() {
				logger.Warnf("send request time out")
			} else {
				logger.Warnf("send request fail")
			}
		}
	}

	api.remotingClient.InvokeAsync(addr, request, timeout, invokeCallback)
}

func (api *mqClientAPI) processPullResponse(response *protocol.RemotingCommand) *PullResultExt {
	pullStatus := common.NO_NEW_MSG
	switch response.Code {
	case protocol.SUCCESS:
		pullStatus = common.FOUND
	case protocol.PULL_NOT_FOUND:
		pullStatus = common.NO_NEW_MSG
	case protocol.PULL_RETRY_IMMEDIATELY:
		pullStatus = common.NO_MATCHED_MSG
	case protocol.PULL_OFFSET_MOVED:
		pullStatus = common.OFFSET_ILLEGAL
	}

	respHeader := &head.PullMessageResponseHeader{}
	response.DecodeCommandCustomHeader(respHeader)
	return &PullResultExt{
		PullResult: &common.PullResult{
			Status:          pullStatus,
			NextBeginOffset: respHeader.NextBeginOffset,
			MinOffset:       respHeader.MinOffset,
			MaxOffset:       respHeader.MaxOffset,
		},
		suggestWhichBrokerId: respHeader.SuggestWhichBrokerId,
		messageBinary:        response.Body,
	}
}

func (api *mqClientAPI) updateConsumerOffsetOneway(addr string, header head.UpdateConsumerOffsetRequestHeader, timeout int64) error {
	if api.groupPrefix != "" {
		header.ConsumerGroup = common.BuildWithProjectGroup(header.ConsumerGroup, api.groupPrefix)
		header.Topic = common.BuildWithProjectGroup(header.Topic, api.groupPrefix)
	}

	request := protocol.CreateRequestCommand(protocol.UPDATE_CONSUMER_OFFSET, &header)
	// oneway处理
	request.MarkOnewayRPC()
	return api.remotingClient.InvokeOneway(addr, request, timeout)
}

func (api *mqClientAPI) getConsumerIdListByGroup(addr string, consumerGroup string, timeout int64) ([]string, error) {
	if api.groupPrefix != "" {
		consumerGroup = common.BuildWithProjectGroup(consumerGroup, api.groupPrefix)
	}

	header := head.GetConsumersByGroupRequestHeader{ConsumerGroup: consumerGroup}
	request := protocol.CreateRequestCommand(protocol.GET_CONSUMER_LIST_BY_GROUP, &header)
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, errors.Errorf("get consumerId list by group err: response is nil.")
	}

	if response.Code != protocol.SUCCESS {
		return nil, errors.Errorf("get consumerId list by group failed, code:%d, remark:%s.", response.Code, response.Remark)
	}

	if len(response.Body) == 0 {
		return []string{}, nil
	}

	consumersBody := &body.GetConsumersByGroupResponse{}
	err = common.Decode(response.Body, consumersBody)
	if err != nil {
		return nil, err
	}

	return consumersBody.ConsumerIdList, nil
}

// 获取队列最大offset
func (api *mqClientAPI) getMaxOffset(addr string, topic string, queueId int, timeout int64) (int64, error) {
	if api.groupPrefix != "" {
		topic = common.BuildWithProjectGroup(topic, api.groupPrefix)
	}

	header := head.GetMaxOffsetRequestHeader{Topic: topic, QueueId: queueId}
	request := protocol.CreateRequestCommand(protocol.GET_MAX_OFFSET, &header)
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return 0, err
	}

	if response == nil {
		return 0, errors.Errorf("get max offset err: response is nil.")
	}

	if response.Code != protocol.SUCCESS {
		return 0, errors.Errorf("get max offset failed, code:%d, remark:%s.", response.Code, response.Remark)
	}

	respHeader := &head.GetMaxOffsetResponseHeader{}
	response.DecodeCommandCustomHeader(respHeader)
	return respHeader.Offset, nil
}

func (api *mqClientAPI) queryConsumerOffset(addr string, header head.QueryConsumerOffsetRequestHeader, timeout int64) (int64, error) {
	if api.groupPrefix != "" {
		header.ConsumerGroup = common.BuildWithProjectGroup(header.ConsumerGroup, api.groupPrefix)
		header.Topic = common.BuildWithProjectGroup(header.Topic, api.groupPrefix)
	}

	request := protocol.CreateRequestCommand(protocol.QUERY_CONSUMER_OFFSET, &header)
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return 0, err
	}

	if response == nil {
		return 0, errors.Errorf("query consumer offset err: response is nil.")
	}

	if response.Code != protocol.SUCCESS {
		return 0, errors.Errorf("query consumer offset failed, code:%d, remark:%s.", response.Code, response.Remark)
	}

	respHeader := &head.QueryConsumerOffsetResponseHeader{}
	response.DecodeCommandCustomHeader(respHeader)
	return respHeader.Offset, nil
}

func (api *mqClientAPI) updateNameServerAddressList(addrs []string) {
	api.remotingClient.UpdateNameServerAddressList(addrs)
}

func (api *mqClientAPI) consumerSendMessageBack(addr string, msg *message.MessageExt,
	consumerGroup string, delayLevel int32, timeout int64) error {
	if api.groupPrefix != "" {
		consumerGroup = common.BuildWithProjectGroup(consumerGroup, api.groupPrefix)
		msg.Topic = common.BuildWithProjectGroup(msg.Topic, api.groupPrefix)
	}

	header := head.ConsumerSendMsgBackRequestHeader{
		Group:       consumerGroup,
		OriginTopic: msg.Topic,
		Offset:      msg.CommitLogOffset,
		DelayLevel:  delayLevel,
		OriginMsgId: msg.MsgId,
	}
	request := protocol.CreateRequestCommand(protocol.CONSUMER_SEND_MSG_BACK, &header)
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return err
	}

	if response == nil {
		return errors.Errorf("consumer send message back err: response is nil.")
	}

	if response.Code != protocol.SUCCESS {
		return errors.Errorf("consumer send message back failed, code:%d, remark:%s.", response.Code, response.Remark)
	}

	return nil
}

func (api *mqClientAPI) unRegisterClient(addr, clientID, producerGroup, consumerGroup string, timeout int64) error {
	if api.groupPrefix != "" {
		producerGroup = common.BuildWithProjectGroup(producerGroup, api.groupPrefix)
		consumerGroup = common.BuildWithProjectGroup(consumerGroup, api.groupPrefix)
	}

	header := &head.UnRegisterClientRequestHeader{ClientID: clientID, ProducerGroup: producerGroup, ConsumerGroup: consumerGroup}
	request := protocol.CreateRequestCommand(protocol.UNREGISTER_CLIENT, header)
	response, err := api.remotingClient.InvokeSync(addr, request, timeout)
	if err != nil {
		return err
	}

	if response == nil {
		return errors.Errorf("unregister client err: response is nil.")
	}

	if response.Code != protocol.SUCCESS {
		return errors.Errorf("unregister client failed, code:%d, remark:%s.", response.Code, response.Remark)
	}

	return nil
}

// 创建topic
func (api *mqClientAPI) createTopic(brokerAddr, defaultTopic string, topicConfig *base.TopicConfig, timeout int) error {
	topic := topicConfig.TopicName
	if api.groupPrefix != "" {
		topic = common.BuildWithProjectGroup(topicConfig.TopicName, api.groupPrefix)
	}

	// 此处的defaultTopic表示创建topic的key值，真正的topic名称是位于topicConfig.Topic字段
	header := head.NewCreateTopicRequestHeader(topic, defaultTopic, topicConfig)
	request := protocol.CreateRequestCommand(protocol.UPDATE_AND_CREATE_TOPIC, header)
	response, err := api.remotingClient.InvokeSync(brokerAddr, request, int64(timeout))
	if err != nil {
		return errors.Errorf("create topic[%s] err: %s", topicConfig.TopicName, err)
	}

	if response == nil {
		return errors.Errorf("create topic[%s] response is nil", topicConfig.TopicName)
	}

	if response.Code != protocol.SUCCESS {
		return errors.Errorf("create topic[%s] failed. code: %s, remark: %s.", topicConfig.TopicName, response.Code, response.Remark)
	}

	return nil
}

// 获取配置信息
func (api *mqClientAPI) getKVConfigValue(namespace, key string, timeout int64) (string, error) {
	header := &head.GetKVConfigRequestHeader{Namespace: namespace, Key: key}
	request := protocol.CreateRequestCommand(protocol.GET_KV_CONFIG, header)

	response, err := api.remotingClient.InvokeSync("", request, timeout)
	if err != nil {
		return "", errors.Errorf("Get KVConfig Value err: %s, the request is %s", err, request)
	}

	if response == nil {
		return "", errors.Errorf("Get KVConfig Value response is nil")
	}

	if response.Code != protocol.SUCCESS {
		return "", errors.Errorf("Get KVConfig Value failed, Code:%d.", response.Code)
	}

	respHeader := &head.GetKVConfigResponseHeader{}
	err = response.DecodeCommandCustomHeader(respHeader)
	if err != nil {
		return "", errors.Errorf("Decode Get KVConfig Response Header err: %s", err)
	}

	return respHeader.Value, nil
}

// 设置配置信息
func (api *mqClientAPI) putKVConfigValue(namespace, key, value string, timeout int64) error {
	header := &head.PutKVConfigRequestHeader{Namespace: namespace, Key: key, Value: value}
	request := protocol.CreateRequestCommand(protocol.PUT_KV_CONFIG, header)
	nameSrvAddrs := api.remotingClient.GetNameServerAddressList()
	if nameSrvAddrs == nil || len(nameSrvAddrs) == 0 {
		return errors.Errorf("not used name server addr.")
	}

	var g errgroup.Group
	for _, addr := range nameSrvAddrs {
		response, err := api.remotingClient.InvokeSync(addr, request, timeout)
		if err != nil {
			return err
		}

		if response == nil {
			return errors.Errorf("put kv config value response is nil.")
		}

		if response.Code != protocol.SUCCESS {
			g.Error(errors.Errorf("addr:%s code:%d remark:%s", addr, response.Code, response.Remark))
		}
	}

	if err := g.Wait(); err != nil {
		//errs, ok := err.(errgroup.MultiError);
		return err
	}

	return nil
}

// 获取配置信息
func (api *mqClientAPI) getKVListByNamespace(namespace string, timeout int64) (*protocol.KVTable, error) {
	header := &head.GetKVListByNamespaceRequestHeader{Namespace: namespace}
	request := protocol.CreateRequestCommand(protocol.GET_KVLIST_BY_NAMESPACE, header)
	response, err := api.remotingClient.InvokeSync("", request, timeout)
	if err != nil {
		return nil, err
	}

	if response == nil {
		return nil, errors.Errorf("get kvList by namespace err: response is nil.")
	}
	if response.Code != protocol.SUCCESS {
		return nil, errors.Errorf("get kvList by namespace failed, code:%d remark:%s.", response.Code, response.Remark)
	}

	kvTable := protocol.NewKVTable()
	err = common.Decode(response.Body, kvTable)
	if err != nil {
		return nil, errors.Errorf("decode kvList by namespace err: %s.", err)
	}

	return kvTable, nil
}

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
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/net/remoting"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/protocol/namesrv"
	"github.com/boltmq/sdk/common"
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

// 发送心跳到broker
func (api *mqClientAPI) sendHeartbeat(addr string, heartbeatData *heartbeat.HeartbeatData, timeout int64) error {
	if api.groupPrefix != "" {
		for _, cData := range heartbeatData.ConsumerDatas {
			cData.GroupName = buildWithProjectGroup(cData.GroupName, api.groupPrefix)
			for _, subData := range cData.SubscriptionDatas {
				subData.Topic = buildWithProjectGroup(subData.Topic, api.groupPrefix)
			}
		}

		for _, pData := range heartbeatData.ProducerDatas {
			pData.GroupName = buildWithProjectGroup(pData.GroupName, api.groupPrefix)
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
		topic = buildWithProjectGroup(topic, api.groupPrefix)
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
		msg.Topic = buildWithProjectGroup(msg.Topic, api.groupPrefix)
		header.ProducerGroup = buildWithProjectGroup(header.ProducerGroup, api.groupPrefix)
		header.Topic = buildWithProjectGroup(header.Topic, api.groupPrefix)
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

func (api *mqClientAPI) processSendResponse(brokerName string, msg *message.Message, response *protocol.RemotingCommand) (*Result, error) {
	return nil, nil
}

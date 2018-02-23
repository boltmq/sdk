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
	"fmt"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/sdk/common"
)

// 客户端处理器
type remotingProcessor struct {
	mqClient *MQClient
}

func newRemotingProcessor(mqClient *MQClient) *remotingProcessor {
	return &remotingProcessor{
		mqClient: mqClient,
	}
}

// 处理request
func (processor *remotingProcessor) ProcessRequest(ctx core.Context,
	request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	case protocol.NOTIFY_CONSUMER_IDS_CHANGED:
		return processor.notifyConsumerIdsChanged(ctx, request)
	case protocol.CONSUME_MESSAGE_DIRECTLY:
		return processor.consumeMessageDirectly(ctx, request)
	default:
	}

	return nil, nil
}

func (processor *remotingProcessor) notifyConsumerIdsChanged(ctx core.Context,
	request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	header := &head.NotifyConsumerIdsChangedRequestHeader{}
	err := request.DecodeCommandCustomHeader(header)
	if err != nil {
		return response, err
	}

	logger.Infof("receive broker's notification[%s], the consumer group: %s changed, rebalance immediately",
		ctx.RemoteAddr(), header.ConsumerGroup)
	processor.mqClient.rebalanceImmediately()

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

func (processor *remotingProcessor) consumeMessageDirectly(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	header := &head.ConsumeMessageDirectlyResultRequestHeader{}
	err := request.DecodeCommandCustomHeader(header)
	if err != nil {
		return response, err
	}

	msg, err := message.DecodeMessageExt(request.Body, true, true)
	if err != nil {
		return response, err
	}

	result := processor.mqClient.ConsumeMessageDirectly(msg, header.ConsumerGroup, header.BrokerName)
	if result != nil {
		response.Code = protocol.SUCCESS
		body, err := common.Encode(result)
		if err != nil {
			return response, err
		}

		response.Body = body
		return response, nil
	}

	response.Remark = fmt.Sprintf("The Consumer Group <%s> not exist in this consumer", header.ConsumerGroup)
	return response, nil
}

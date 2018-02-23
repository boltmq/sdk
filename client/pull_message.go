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
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
)

type PullRequest struct {
	ConsumerGroup string
	NextOffset    int64
	messageQueue  *message.MessageQueue
	processQueue  *ProcessQueue
}

type pullMessageService struct {
	mqClient  *MQClient
	prCh      chan *PullRequest
	isStopped bool
}

func newPullMessageService(mqClient *MQClient) *pullMessageService {
	return &pullMessageService{
		mqClient: mqClient,
		prCh:     make(chan *PullRequest)}
}

func (service *pullMessageService) start() {
	go func() {
		service.run()
	}()
}
func (service *pullMessageService) shutdown() {
	service.isStopped = true
}

// 向通道中加入pullRequest
func (service *pullMessageService) ExecutePullRequestImmediately(pullRequest *PullRequest) {
	service.prCh <- pullRequest
}

// 延迟执行pull请求
func (service *pullMessageService) ExecutePullRequestLater(pullRequest *PullRequest, timeDelay int) {
	go func() {
		time.Sleep(time.Millisecond * time.Duration(timeDelay))
		service.ExecutePullRequestImmediately(pullRequest)
	}()
}

func (service *pullMessageService) run() {
	logger.Info("pull message service started")
	for !service.isStopped {
		request := <-service.prCh
		service.pullMessage(request)
	}
}

func (service *pullMessageService) pullMessage(pullRequest *PullRequest) {
	mConsumer := service.mqClient.selectConsumer(pullRequest.ConsumerGroup)
	if mConsumer != nil {
		//TODO:
		//mConsumer.(*DefaultMQPushConsumerImpl).pullMessage(pullRequest)
	}
}

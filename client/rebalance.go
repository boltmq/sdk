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

import "time"

// 消费端rebalance服务
type rebalanceService struct {
	mqClient     *MQClient
	waitInterval int //单位秒
	isStoped     bool
	wakeup       chan struct{}
}

func newRebalanceService(mqClient *MQClient) *rebalanceService {
	return &rebalanceService{
		mqClient:     mqClient,
		waitInterval: 10,
		wakeup:       make(chan struct{}, 1),
	}
}

func (service *rebalanceService) start() {
	go func() {
		for !service.isStoped {
			select {
			case <-time.After(time.Second * time.Duration(service.waitInterval)):
				service.mqClient.doRebalance()
			case <-service.wakeup:
				service.mqClient.doRebalance()
			}
		}
	}()
}

func (service *rebalanceService) shutdown() {
	service.isStoped = true
}

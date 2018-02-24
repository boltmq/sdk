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
	"sync/atomic"
)

var clientManager *mqClientManager

func init() {
	clientManager = newMQClientManager()
}

// MQClient管理器
type mqClientManager struct {
	clientTable   map[string]*MQClient // key: clientId
	clientTableMu sync.Mutex
	autoIndex     int32
}

func newMQClientManager() *mqClientManager {
	return &mqClientManager{
		clientTable: make(map[string]*MQClient),
	}
}

// 查询MQClient，无则创建一个
func (manager *mqClientManager) getAndCreateMQClient(cfg *Config) *MQClient {
	clientId := cfg.BuildClientId()

	manager.clientTableMu.Lock()
	defer manager.clientTableMu.Unlock()
	if mqClient, ok := manager.clientTable[clientId]; ok {
		return mqClient
	}

	mqClient := NewMQClient(*cfg,
		atomic.AddInt32(&manager.autoIndex, 1), clientId)
	manager.clientTable[clientId] = mqClient

	return mqClient
}

// 删除客户端
func (manager *mqClientManager) removeMQClient(clientId string) {
	manager.clientTableMu.Lock()
	delete(manager.clientTable, clientId)
	manager.clientTableMu.Unlock()
}

// GetAndCreateMQClient 查询MQClient，无则创建一个
func GetAndCreateMQClient(cfg *Config) *MQClient {
	return clientManager.getAndCreateMQClient(cfg)
}

// RemoveMQClient 删除客户端
func RemoveMQClient(clientId string) {
	clientManager.removeMQClient(clientId)
}

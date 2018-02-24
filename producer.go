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
package boltmq

import (
	"github.com/boltmq/common/message"
	"github.com/boltmq/sdk/common"
)

const (
	SEND_OK common.ResultStatus = common.SEND_OK
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
)

type Result = common.Result

type Callback = common.Callback

type Producer interface {
	NameSrvAddrs(addrs []string)
	InstanceName(instanceName string)
	Send(msg *message.Message) (*Result, error)
	SendOneWay(msg *message.Message) error
	SendCallBack(msg *message.Message, callback Callback) error
	Start() error
	Stop()
}

func NewProduer(producerGroup string) Producer {
	return nil
}

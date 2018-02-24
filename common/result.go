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
package common

import (
	"fmt"

	"github.com/boltmq/common/message"
)

type Callback func(r *Result, err error)

type Result struct {
	Status      ResultStatus
	MsgId       string
	Queue       *message.MessageQueue
	QueueOffset int64
	TransId     string
}

func NewResult(groupPrefix string, status ResultStatus, msgId string, messageQueue *message.MessageQueue,
	queueOffset int64, transId string) *Result {
	if groupPrefix != "" {
		messageQueue.Topic = ClearProjectGroup(messageQueue.Topic, groupPrefix)
	}

	return &Result{
		Status:      status,
		MsgId:       msgId,
		Queue:       messageQueue,
		QueueOffset: queueOffset,
		TransId:     transId,
	}
}

func (r *Result) String() string {
	return fmt.Sprintf("Resut[MsgId:%s, Status:%s, Queue:%s, QueueOffset:%d, TransId:%d]",
		r.MsgId, r.Status, r.Queue, r.QueueOffset, r.TransId)
}

type ResultStatus int

const (
	SEND_OK ResultStatus = iota
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
	SEND_UNKNOW
)

func (status ResultStatus) String() string {
	switch status {
	case SEND_OK:
		return "SEND_OK"
	case FLUSH_DISK_TIMEOUT:
		return "FLUSH_DISK_TIMEOUT"
	case FLUSH_SLAVE_TIMEOUT:
		return "FLUSH_SLAVE_TIMEOUT"
	case SLAVE_NOT_AVAILABLE:
		return "SLAVE_NOT_AVAILABLE"
	default:
		return "Unknow"
	}
}

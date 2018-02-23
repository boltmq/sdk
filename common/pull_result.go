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

import "github.com/boltmq/common/message"

type PullResult struct {
	Status          PullStatus
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	Msgs            []*message.MessageExt
}

type PullStatus int

const (
	// Founded
	FOUND PullStatus = iota
	// No new message can be pull
	NO_NEW_MSG
	// Filtering results can not match
	NO_MATCHED_MSG
	// Illegal offset，may be too big or too small
	OFFSET_ILLEGAL
)

func (status PullStatus) String() string {
	switch status {
	case FOUND:
		return "FOUND"
	case NO_NEW_MSG:
		return "NO_NEW_MSG"
	case NO_MATCHED_MSG:
		return "NO_MATCHED_MSG"
	case OFFSET_ILLEGAL:
		return "OFFSET_ILLEGAL"
	default:
		return "Unknow"
	}
}

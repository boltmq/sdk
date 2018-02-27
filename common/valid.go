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
	"regexp"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/message"
	"github.com/go-errors/errors"
)

const (
	CHARACTER_MAX_LENGTH = 255
	VALID_PATTERN_STR    = "^[%|a-zA-Z0-9_-]+$"
)

func CheckGroup(group string) error {
	if group == "" {
		return errors.New("the specified group is blank")
	}

	if len(group) > CHARACTER_MAX_LENGTH {
		return errors.New("the specified group is longer than group max length 255")
	}

	return nil
}

func CheckMessage(msg *message.Message, maxSize int) error {
	if len(msg.Body) == 0 {
		return errors.New("the message body length is zero")
	}

	if len(msg.Body) > maxSize {
		return errors.Errorf("the message body size over max value, MAX: %d", maxSize)
	}

	return CheckTopic(msg.Topic)
}

func CheckTopic(topic string) error {
	if topic == "" {
		return errors.New("the specified topic is blank")
	}

	if ok, _ := regexp.MatchString(VALID_PATTERN_STR, topic); !ok {
		return errors.Errorf("the specified topic[%s] contains illegal characters, allowing only %s", topic, VALID_PATTERN_STR)
	}

	if len([]rune(topic)) > CHARACTER_MAX_LENGTH {
		return errors.New("the specified topic is longer than topic max length 255.")
	}

	if basis.DEFAULT_TOPIC == topic {
		return errors.Errorf(fmt.Sprintf("the topic[%s] is conflict with default topic.", topic))
	}

	return nil
}

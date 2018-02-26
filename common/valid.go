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
	"github.com/go-errors/errors"
)

const (
	CHARACTER_MAX_LENGTH = 255
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

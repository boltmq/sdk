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
	"os"
	"strconv"
	"time"

	"github.com/boltmq/common/utils/codec"
)

type Config struct {
	NameSrvAddrs                  []string
	InstanceName                  string
	ClientIP                      string
	ClientCallbackExecutorThreads int
	PullNameServerInteval         int
	HeartbeatBrokerInterval       int
	PersistConsumerOffsetInterval int
}

// BuildClientId
func (cfg *Config) BuildClientId() string {
	unixNano := time.Now().UnixNano()
	hash := uint64(codec.HashCode(cfg.NameSrvAddrs))
	return fmt.Sprintf("%s@%d#%d#%d", cfg.ClientIP, os.Getpid(), hash, unixNano)
}

func (cfg *Config) ChangeInstanceNameToPID() {
	if cfg.InstanceName == "DEFAULT" {
		cfg.InstanceName = strconv.Itoa(os.Getpid())
	}
}

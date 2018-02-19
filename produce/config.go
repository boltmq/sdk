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
package produce

type config struct {
	createTopic                      string
	topicQueueNums                   int
	sendMsgTimeout                   int64
	compressMsgBodyOverHowmuch       int
	retryTimesWhenSendFailed         int32
	retryAnotherBrokerWhenNotStoreOK bool
	maxMessageSize                   int
	unitMode                         bool
	client                           clientConfig
}

type clientConfig struct {
	nameSrvAddrs                  []string
	instanceName                  string
	clientIP                      string
	clientCallbackExecutorThreads int
	pullNameServerInterval        int
	heartbeatBrokerInterval       int
	persistConsumerOffsetInterval int
}

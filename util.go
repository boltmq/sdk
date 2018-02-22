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
	"net"
	"os"
	"strings"

	"github.com/go-errors/errors"
)

func defaultLocalAddress() string {
	if laddr, err := localAddress(); err == nil {
		return laddr
	}

	return ""
}

func localAddress() (laddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return laddr, err
	}

	for _, addr := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && !isIntranetIpv4(ipnet.IP.String()) {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", errors.Errorf("<none>")
}

func isIntranetIpv4(ip string) bool {
	//if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "169.254.") {
	if strings.HasPrefix(ip, "169.254.") {
		return true
	}
	return false
}

func defaultInstanceName() string {
	instanceName := os.Getenv("BOLTMQ_CLIENT_NAME")
	if instanceName == "" {
		instanceName = "DEFAULT"
	}

	return instanceName
}

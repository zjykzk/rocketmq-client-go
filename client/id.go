package client

import "strings"

// BuildMQClientID build the mq client ID
func BuildMQClientID(ip, unitName, instanceName string) string {
	id := ip + "@" + instanceName
	if unitName := strings.Trim(unitName, " "); unitName != "" {
		return id + "@" + unitName
	}
	return id
}

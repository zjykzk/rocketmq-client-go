package rocketmq

import (
	"errors"
	"net"
)

// GetIP returns local address
func GetIP() ([]byte, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.New("unknow_ip")
	}

	var internalIP []byte
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}

		ip := ipnet.IP
		if ip.IsLoopback() {
			continue
		}

		if ip = ip.To4(); ip == nil {
			continue
		}

		if checkIP(ip) {
			if !isInternalIP(ip) {
				return ip, nil
			}

			internalIP = ip
		}
	}

	if internalIP != nil {
		return internalIP, nil
	}
	return nil, errors.New("unknow_ip")
}

// GetIPStr return the ip address as readable style
func GetIPStr() (string, error) {
	ip, err := getIP()
	if err != nil {
		return "", err
	}

	return ip.String(), nil
}

func getIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.New("unknow_ip")
	}

	var internalIP net.IP
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}

		ip := ipnet.IP
		if ip.IsLoopback() {
			continue
		}

		if ip = ip.To4(); ip == nil {
			continue
		}

		if checkIP(ip) {
			if !isInternalIP(ip) {
				return ip, nil
			}

			internalIP = ip
		}
	}

	if internalIP != nil {
		return internalIP, nil
	}
	return nil, errors.New("unknow_ip")
}

func checkIP(ip []byte) bool {
	if ip[0] >= 1 && ip[0] <= 126 {
		if ip[1] == 1 && ip[2] == 1 && ip[3] == 1 {
			return false
		}
		if ip[1] == 0 && ip[2] == 0 && ip[3] == 0 {
			return false
		}
		return true
	}

	if ip[0] >= 128 && ip[0] <= 191 {
		if ip[2] == 1 && ip[3] == 1 {
			return false
		}
		if ip[2] == 0 && ip[3] == 0 {
			return false
		}
		return true
	}

	if ip[0] >= 192 && ip[0] <= 223 {
		if ip[3] == 1 {
			return false
		}
		if ip[3] == 0 {
			return false
		}
		return true
	}
	return false
}

func isInternalIP(ip []byte) bool {
	//10.0.0.0~10.255.255.255
	//172.16.0.0~172.31.255.255
	//192.168.0.0~192.168.255.255
	switch {
	case ip[0] == 10:
		return true
	case ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31:
		return true
	case ip[0] == 192 && ip[1] == 168:
		return true
	default:
		return false
	}
}

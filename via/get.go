package via

import (
	"strings"

	"github.com/byuoitav/common/log"

	"github.com/byuoitav/common/nerr"
	"github.com/byuoitav/common/structs"
	"github.com/fatih/color"
)

// User status constants
const (
	Inactive = "0"
	Active   = "1"
	Waiting  = "2"
)

// IsConnected checks the status of the VIA connection
func IsConnected(address string) bool {
	defer color.Unset()
	color.Set(color.FgYellow)
	connected := false

	log.L.Infof("Getting connected status of %s", address)

	var command Command
	resp, err := SendCommand(command, address)
	if err == nil && strings.Contains(resp, "Successful") {
		connected = true
	}

	return connected
}

// GetVolume for a VIA device
func GetVolume(address string) (int, error) {

	defer color.Unset()
	color.Set(color.FgYellow)

	var command Command
	command.Command = "Vol"
	command.Param1 = "Get"

	log.L.Infof("Sending command to get VIA Volume to %s", address)
	// Note: Volume Get command in VIA API doesn't have any error handling so it only returns Vol|Get|XX or nothing
	// I am still checking for errors just in case something else fails during execution
	vollevel, _ := SendCommand(command, address)

	return VolumeParse(vollevel)
}

// GetHardwareInfo for a VIA device
func GetHardwareInfo(address string) (structs.HardwareInfo, *nerr.E) {
	defer color.Unset()
	color.Set(color.FgYellow)

	log.L.Infof("Getting hardware info of %s", address)

	var toReturn structs.HardwareInfo
	var command Command

	// get serial number
	command.Command = "GetSerialNo"

	serial, err := SendCommand(command, address)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("failed to get serial number from %s", address)
	}

	toReturn.SerialNumber = parseResponse(serial, "|")

	// get firmware version
	command.Command = "GetVersion"

	version, err := SendCommand(command, address)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("failed to get the firmware version of %s", address)
	}

	toReturn.FirmwareVersion = parseResponse(version, "|")

	// get MAC address
	command.Command = "GetMacAdd"

	macAddr, err := SendCommand(command, address)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("failed to get the MAC address of %s", address)
	}

	// get IP information
	command.Command = "IpInfo"

	ipInfo, err := SendCommand(command, address)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("failed to get the IP information from %s", address)
	}

	hostname, network := parseIPInfo(ipInfo)

	toReturn.Hostname = hostname
	network.MACAddress = parseResponse(macAddr, "|")
	toReturn.NetworkInfo = network

	return toReturn, nil
}

func parseResponse(resp string, delimiter string) string {
	pieces := strings.Split(resp, delimiter)

	var msg string

	if len(pieces) < 2 {
		msg = pieces[0]
	} else {
		msg = pieces[1]
	}

	return strings.Trim(msg, "\r\n")
}

func parseIPInfo(ip string) (hostname string, network structs.NetworkInfo) {
	ipList := strings.Split(ip, "|")

	for _, item := range ipList {
		if strings.Contains(item, "IP") {
			network.IPAddress = strings.Split(item, ":")[1]
		}
		if strings.Contains(item, "GAT") {
			network.Gateway = strings.Split(item, ":")[1]
		}
		if strings.Contains(item, "DNS") {
			network.DNS = []string{strings.Split(item, ":")[1]}
		}
		if strings.Contains(item, "Host") {
			hostname = strings.Trim(strings.Split(item, ":")[1], "\r\n")
		}
	}

	return hostname, network
}

// GetStatusOfUsers returns the status of users that are logged in to the VIA
func GetStatusOfUsers(address string) (structs.VIAUsers, *nerr.E) {
	var toReturn structs.VIAUsers
	toReturn.InactiveUsers = []string{}
	toReturn.ActiveUsers = []string{}
	toReturn.UsersWaiting = []string{}

	defer color.Unset()
	color.Set(color.FgYellow)

	var command Command
	command.Command = "PList"
	command.Param1 = "all"
	command.Param2 = "4"

	log.L.Infof("Sendind command to get VIA users info to %s", address)

	response, err := SendCommand(command, address)
	if err != nil {
		return toReturn, nerr.Translate(err).Addf("failed to get user information from %s", address)
	}

	fullList := strings.Split(response, "|")

	userList := strings.Split(fullList[3], "#")

	for _, user := range userList {
		log.L.Info(user)
		if len(user) == 0 {
			continue
		}

		userSplit := strings.Split(user, "_")

		if len(userSplit) < 2 {
			continue
		}

		nickname := userSplit[0]
		state := userSplit[1]

		switch state {
		case Inactive:
			toReturn.InactiveUsers = append(toReturn.InactiveUsers, nickname)
			break
		case Active:
			toReturn.ActiveUsers = append(toReturn.ActiveUsers, nickname)
			break
		case Waiting:
			toReturn.UsersWaiting = append(toReturn.UsersWaiting, nickname)
			break
		}
	}

	return toReturn, nil
}

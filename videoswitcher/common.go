package videoswitcher

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
)

const (
	CARRIAGE_RETURN           = 0x0D
	LINE_FEED                 = 0x0A
	SPACE                     = 0x20
	DELAY_BETWEEN_CONNECTIONS = time.Second * 10
)

type Response struct {
	Response string
	Err      error
}

type CommandInfo struct {
	ResponseChannel chan Response
	Address         string
	Command         string
	ReadWelcome     bool
}

var StartChannel = make(chan CommandInfo, 1000)

var connMap = make(map[string]chan CommandInfo)

func StartRouter() {

	stopChannel := make(chan string, 100)
	for {
		select {
		case command, ok := <-StartChannel:
			if !ok {
				color.Set(color.FgRed)
				log.Printf("routing channel was closed")
				return
			}

			if channel, ok := connMap[command.Address]; ok {
				//dump it and move on
				channel <- command
				continue
			}

			newChannel := make(chan CommandInfo, 1000)
			newChannel <- command
			connMap[command.Address] = newChannel

			go startRoutine(newChannel, stopChannel, command.Address)
		case addr := <-stopChannel:

			color.Set(color.FgRed)
			log.Printf("Close message received for %v", addr)
			color.Unset()

			close(connMap[addr])
			delete(connMap, addr)
		}
	}
}

func startRoutine(channel chan CommandInfo, stopChannel chan string, address string) {

	conn, err := getConnection(address)
	if err != nil {
		logError(fmt.Sprintf("could not get connection for %v: %v", address, err.Error()))
		return
	}

	timer := time.NewTimer(25 * time.Second)

	for {
		select {
		case command, ok := <-channel:
			if !ok {
				color.Set(color.FgRed)
				log.Printf("channel for addr: %v was closed", address)
				return
			}

			resp, err := SendCommand(command.Address, command.Command, command.ReadWelcome, conn)
			command.ResponseChannel <- Response{Response: resp, Err: err}

			timer.Reset(25 * time.Second)
		case <-timer.C:
			color.Set(color.FgRed)
			log.Printf("timer expired for %v, sending close message.", address)
			color.Unset()
			stopChannel <- address
		}
	}
}

// Takes a command and sends it to the address, and returns the devices response to that command
func SendCommand(address, command string, readWelcome bool, conn *net.TCPConn) (resp string, err error) {
	defer color.Unset()

	if !readWelcome {
		resp, err = writeCommand(conn, command)
		if err != nil {
			return "", err
		}

	} else {
		// read the welcome message
		if readWelcome {
			color.Set(color.FgMagenta)
			log.Printf("Reading welcome message")
			color.Unset()
			_, err := readUntil(CARRIAGE_RETURN, conn, 3)
			if err != nil {
				return "", err
			}
		}

		// write command
		resp, err = writeCommand(conn, command)
		if err != nil {
			return "", err
		}
	}

	color.Set(color.FgBlue)
	log.Printf("Response from device: %s", resp)
	return resp, nil
}

func writeCommand(conn *net.TCPConn, command string) (string, error) {
	command = strings.Replace(command, " ", string(SPACE), -1)
	color.Set(color.FgMagenta)
	log.Printf("Sending command %s", command)
	color.Unset()
	command += string(CARRIAGE_RETURN) + string(LINE_FEED)
	conn.Write([]byte(command))

	// get response
	resp, err := readUntil(LINE_FEED, conn, 5)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

// This function converts a number (in a string) to index-based 1.
func ToIndexOne(numString string) (string, error) {
	num, err := strconv.Atoi(numString)
	if err != nil {
		return "", err
	}

	// add one to make it match pulse eight.
	// we are going to use 0 based indexing on video matrixing,
	// and the kramer uses 1-based indexing.
	num++

	return strconv.Itoa(num), nil
}

// Returns if a given number (in a string) is less than zero.
func LessThanZero(numString string) bool {
	defer color.Unset()
	num, err := strconv.Atoi(numString)
	if err != nil {
		color.Set(color.FgRed)
		log.Printf("Error converting %s to a number: %s", numString, err.Error())
		return false
	}

	return num < 0
}

// This function converts a number (in a string) to index-base 0.
func ToIndexZero(numString string) (string, error) {
	num, err := strconv.Atoi(numString)
	if err != nil {
		return "", err
	}

	num--

	return strconv.Itoa(num), nil
}

func getConnection(address string) (*net.TCPConn, error) {
	color.Set(color.FgMagenta)
	log.Printf("Opening telnet connection with %s", address)
	color.Unset()

	addr, err := net.ResolveTCPAddr("tcp", address+":5000")
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func readUntil(delimeter byte, conn *net.TCPConn, timeoutInSeconds int) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(time.Duration(int64(timeoutInSeconds)) * time.Second))

	buffer := make([]byte, 128)
	message := []byte{}

	for !charInBuffer(delimeter, buffer) {
		_, err := conn.Read(buffer)
		if err != nil {
			err = errors.New(fmt.Sprintf("Error reading response: %s", err.Error()))
			log.Printf("%s", err.Error())
			return message, err
		}

		message = append(message, buffer...)
	}

	return removeNil(message), nil
}

func removeNil(b []byte) (ret []byte) {
	for _, c := range b {
		switch c {
		case '\x00':
			break
		default:
			ret = append(ret, c)
		}
	}
	return ret
}

func charInBuffer(toCheck byte, buffer []byte) bool {
	for _, b := range buffer {
		if toCheck == b {
			return true
		}
	}

	return false
}

func logError(e string) {
	color.Set(color.FgRed)
	log.Printf("%s", e)
	color.Unset()
}

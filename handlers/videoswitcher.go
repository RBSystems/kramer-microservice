package handlers

import (
	"fmt"

	"net/http"
	"strconv"

	"github.com/byuoitav/common/log"
	vs "github.com/byuoitav/kramer-microservice/videoswitcher"
	"github.com/fatih/color"
	"github.com/labstack/echo"
)

func SwitchInput(context echo.Context) error {
	// if ok, err := auth.CheckAuthForLocalEndpoints(context, "write-state"); !ok {
	// 	if err != nil {
	// 		log.L.Warnf("Problem getting auth: %v", err.Error())
	// 	}
	// 	return context.String(http.StatusUnauthorized, "unauthorized")
	// }

	defer color.Unset()

	input := context.Param("input")
	output := context.Param("output")
	address := context.Param("address")
	readWelcome, err := strconv.ParseBool(context.Param("bool"))

	if err != nil {
		return context.JSON(http.StatusBadRequest, "Error! welcome must be a true/false value")
	}

	i, err := vs.ToIndexOne(input)
	if err != nil || vs.LessThanZero(input) {
		return context.JSON(http.StatusBadRequest, fmt.Sprintf("Error! Input parameter %s is not valid!", input))
	}

	o, err := vs.ToIndexOne(output)
	if err != nil || vs.LessThanZero(output) {
		return context.JSON(http.StatusBadRequest, "Error! Output parameter must be zero or greater")
	}

	color.Set(color.FgYellow)
	log.L.Debugf("Routing %v to %v on %v", input, output, address)
	log.L.Debugf("Changing to 1-based indexing... (+1 to each port number)")

	ret, err := vs.SwitchInput(address, i, o, readWelcome)
	if err != nil {
		color.Set(color.FgRed)
		log.L.Debugf("There was a problem: %v", err.Error())
		return context.JSON(http.StatusInternalServerError, err.Error())
	}

	color.Set(color.FgYellow)
	log.L.Debugf("Changing to 0-based indexing... (-1 to each port number)")
	ret.Input, err = vs.ToIndexZero(ret.Input)
	if err != nil {
		return context.JSON(http.StatusInternalServerError, err.Error())
	}

	ret.Input = fmt.Sprintf("%v:%v", ret.Input, output)

	color.Set(color.FgGreen, color.Bold)
	log.L.Debugf("Success")
	return context.JSON(http.StatusOK, ret)
}

func GetInputByPort(context echo.Context) error {
	// if ok, err := auth.CheckAuthForLocalEndpoints(context, "write-state"); !ok {
	// 	if err != nil {
	// 		log.L.Warnf("Problem getting auth: %v", err.Error())
	// 	}
	// 	return context.String(http.StatusUnauthorized, "unauthorized")
	// }

	defer color.Unset()

	address := context.Param("address")
	port := context.Param("port")
	readWelcome, err := strconv.ParseBool(context.Param("bool"))
	if err != nil {
		return context.JSON(http.StatusBadRequest, "Error! welcome must be a true/false value")
	}

	p, err := vs.ToIndexOne(port)
	if err != nil || vs.LessThanZero(port) {
		return context.JSON(http.StatusBadRequest, "Error! Port parameter must be zero or greater")
	}

	color.Set(color.FgYellow)
	log.L.Debugf("Getting input for output port %s", port)
	log.L.Debugf("Changing to 1-based indexing... (+1 to each port number)")

	input, err := vs.GetCurrentInputByOutputPort(address, p, readWelcome)
	if err != nil {
		return context.JSON(http.StatusInternalServerError, err.Error())
	}

	color.Set(color.FgYellow)
	log.L.Debugf("Changing to 0-based indexing... (-1 to each port number)")
	input.Input, err = vs.ToIndexZero(input.Input)
	if err != nil {
		return context.JSON(http.StatusInternalServerError, err.Error())
	}

	input.Input = fmt.Sprintf("%v:%v", input.Input, port)
	color.Set(color.FgGreen, color.Bold)
	log.L.Debugf("Input for output port %s is %v", port, input.Input)
	return context.JSON(http.StatusOK, input)
}

func SetFrontLock(context echo.Context) error {
	// if ok, err := auth.CheckAuthForLocalEndpoints(context, "write-state"); !ok {
	// 	if err != nil {
	// 		log.L.Warnf("Problem getting auth: %v", err.Error())
	// 	}
	// 	return context.String(http.StatusUnauthorized, "unauthorized")
	// }

	defer color.Unset()
	address := context.Param("address")
	state, err := strconv.ParseBool(context.Param("bool2"))
	if err != nil {
		return context.JSON(http.StatusBadRequest, "Error! front-button-lock must be set to true/false")
	}
	readWelcome, err := strconv.ParseBool(context.Param("bool"))
	if err != nil {
		return context.JSON(http.StatusBadRequest, "Error! welcome must be a true/false value")
	}

	color.Set(color.FgYellow)
	log.L.Debugf("Setting front button lock status to %v", state)

	err = vs.SetFrontLock(address, state, readWelcome)
	if err != nil {
		return context.JSON(http.StatusInternalServerError, err.Error())
	}

	color.Set(color.FgGreen, color.Bold)
	log.L.Debugf("Success")
	return context.JSON(http.StatusOK, "Success")
}

// GetActiveSignal checks for active signal on a videoswitcher port
func GetActiveSignal(context echo.Context) error {
	address := context.Param("address")
	port := context.Param("port")
	rW := true

	i, err := vs.ToIndexOne(port)
	if err != nil || vs.LessThanZero(port) {
		return context.JSON(http.StatusBadRequest, fmt.Sprintf("Error! Input parameter %s is not valid!", port))
	}

	signal, ne := vs.GetActiveSignalByPort(address, i, rW)
	if ne != nil {
		return context.JSON(http.StatusInternalServerError, err)
	}

	return context.JSON(http.StatusOK, signal)
}

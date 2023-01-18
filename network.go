package main

import (
	"encoding/json"
	"github.com/asmcos/requests"
	_ "github.com/asmcos/requests"
	"github.com/sirupsen/logrus"
)

var (
	authorization = "oIV3f67hzd;eaa828cb7df067424c3211f0811807d6;1673835819"
)

type ResponseInfraredData struct {
	Total        int            `json:"total"`
	InfraredData []InfraredData `json:"data"`
}

type InfraredData struct {
	FTime         int     `json:"f_time"`
	CellsignalRtd int     `json:"cellsignal-Rtd"`
	AlarmTime     int     `json:"Alarm_time"`
	ErrorCode     int     `json:"ErrorCode"`
	DataTime      string  `json:"DataTime"`
	FAEERtd       float64 `json:"FAEE-Rtd"`
	IAEERtd       float64 `json:"IAEE-Rtd"`
	RREERtd       float64 `json:"RREE-Rtd"`
	FREERtd       float64 `json:"FREE-Rtd"`
	EemeidRtd     string  `json:"eemeid-Rtd"`
	IccidRtd      string  `json:"iccid-Rtd"`
	SlbvRtd       float64 `json:"slbv-Rtd"`
	VersionNo     string  `json:"Version_No"`
	MN            string  `json:"MN"`
}

func httpClient(startTime string, endTime string, mn string) ResponseInfraredData {

	var data ResponseInfraredData

	h := requests.Header{
		"Authorization": authorization,
	}

	req := requests.Params{
		"StartTime": startTime,
		"EndTime":   endTime,
		"MN":        mn,
	}
	response, err := requests.Get(g_SysConfig.IotConfig[0].Ioturl, h, req)

	if err != nil {
		logrus.Errorf("network request error:", err)
	}

	err2 := json.Unmarshal(response.Content(), &data)
	if err2 != nil {
		logrus.Errorf("IOT data parse error:", err2)
	}

	return data
}

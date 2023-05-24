package model

// 全局配置参数
var AppConfig AppConfigSt
var SysConfig ConfigParams

// 应用级配置参数
type AppConfigSt struct {
	ConfigDir string
	LogDir    string
	LogLevel  int
}

// 配置文件参数
type ConfigParams struct {
	IotConfig []IotConfig `json:"iot"`
	DbConfig  DbConfig    `json:"db"`
}

type IotConfig struct {
	Name          string   `json:"name"`
	Ioturl        string   `json:"ioturl"`
	Iotmn         string   `json:"iotmn"`
	Iotcode       []string `json:"iotcode"`
	Calctype      int      `json:"calctype"`
	Calcparam     []int    `json:"calcparam"`
	Calcinterval  int      `json:"calcinterval"`
	TransRatioUrl string   `json:"transRatioUrl"`
}

type DbConfig struct {
	CarbonId    string `json:"carbonId"`
	MeterId     string `json:"meterId"`
	MeterType   string `json:"meterType"`
	Flag        string `json:"flag"`
	DeviceType  string `json:"deviceType"`
	StatType    string `json:"statType"`
	Localdbtype int    `json:"localdbtype"`
	Localdburl  string `json:"localdburl"`
}

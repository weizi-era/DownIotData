package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// 应用级配置参数
type AppConfigSt struct {
	configDir string
	logDir    string
	logLevel  int
}

// 配置文件参数
type ConfigParams struct {
	IotConfig []IotConfig `json:"iot"`
	DbConfig  DbConfig    `json:"db"`
}

type IotConfig struct {
	Name         string   `json:"name"`
	Ioturl       string   `json:"ioturl"`
	Iotmn        string   `json:"iotmn"`
	Iotcode      []string `json:"iotcode"`
	Calctype     int      `json:"calctype"`
	Calcparam    []int    `json:"calcparam"`
	Calcinterval int      `json:"calcinterval"`
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

// 全局配置参数
var g_AppConfig AppConfigSt
var g_SysConfig ConfigParams

// 解析命令行参数
func initFlag(pAppConfig *AppConfigSt) {
	flag.StringVar(&pAppConfig.configDir, "configdir", "./etc/", "set config file dir")
	flag.StringVar(&pAppConfig.logDir, "logdir", "./log/", "set log file dir")
	flag.IntVar(&pAppConfig.logLevel, "loglevel", 4, "set log level 0-6 panic fatal error warn info debug trace")

	flag.Parse()
	//fmt.Println(g_AppConfig)
}

// 初始化日志，日志级别0-6 panic fatal error warn info debug trace
func initLog(Logdir string, LogLevel int) {
	if (len(Logdir) > 0) && (Logdir[len(Logdir)-1] == ':') {
		Logdir += "/log/"
	}

	filestr := filepath.Join(Logdir, "downIotData.log") // 文件名固定
	filestr = filepath.FromSlash(filestr)
	filestr = filepath.Clean(filestr)

	logfile := &lumberjack.Logger{
		Filename:   filestr,
		MaxSize:    10, // megabytes
		MaxBackups: 10,
		// MaxAge:     90,	//days
		Compress:  true, // 启用历史日志压缩
		LocalTime: true,
	}

	loggers := io.MultiWriter(logfile, os.Stdout) // 同时记录到文件和标准输出

	//logrus.SetReportCaller(true) // 日志显示调用文件和函数
	// logrus.SetLevel(logrus.DebugLevel)
	loglevel := uint32(LogLevel)
	logrus.SetLevel((logrus.Level)(loglevel))
	// logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetOutput(loggers)
}

func initSysConfig(Configdir string, pConfig *ConfigParams) {

	filestr := filepath.Join(Configdir, "downTotData.json")
	filestr = filepath.FromSlash(filestr)
	filestr = filepath.Clean(filestr)

	configData, err := os.ReadFile(filestr)
	if err != nil {
		logrus.Errorf("配置文件读取失败:", err)
	}

	err = json.Unmarshal(configData, &pConfig)
	if err != nil {
		logrus.Errorf("配置文件解析失败:", err)
	}
}

func main() {

	initFlag(&g_AppConfig)
	if (len(g_AppConfig.configDir) > 0) && (g_AppConfig.configDir[len(g_AppConfig.configDir)-1] == ':') {
		g_AppConfig.configDir += "/etc/"
	}

	initLog(g_AppConfig.logDir, g_AppConfig.logLevel)

	initSysConfig(g_AppConfig.configDir, &g_SysConfig)

	logrus.WithFields(logrus.Fields{"sysconfig": g_SysConfig}).Info("initSysconfig")

	for i, v := range g_SysConfig.IotConfig {
		if len(v.Iotmn) == 0 {
			logrus.WithFields(logrus.Fields{"configindex": i, "hjt212config": v}).
				Error("hjt212config mn slice size != code slice size")
		}

		var dataPicker DataPickerStFctsdb
		dataPicker.id = i
		dataPicker.channelConfig = &g_SysConfig.IotConfig[i]
		dataPicker.dbConfig = &g_SysConfig.DbConfig

		go GetInfoAndInsert(&dataPicker)

		fmt.Println("协程结束")
	}

	for {
		time.Sleep(10 * time.Second)
	}

}

func GetDateTime(date string) int64 {

	loc, _ := time.LoadLocation("Local")
	//日期当天0点时间戳
	zeroTime := date + " 00:00:00"
	//fmt.Println("当前时间:", zeroTime)

	zeroTimeStamp, _ := time.ParseInLocation("2006-01-02 15:04:05", zeroTime, loc)

	//日期当天23时59分时间戳
	/*endDate := date + " 23:59:59"
	end, _ := time.ParseInLocation("2006-01-02_15:04:05", endDate, loc)*/

	return zeroTimeStamp.Unix()
}

func GetInfoAndInsert(dataPicker *DataPickerStFctsdb) {

	//time.Sleep(3 * time.Second)

	var currentTime string
	var currentZeroStamp int64

	var tempDay = 0

	for {
		currentTime = time.Now().Format("2006-01-02")
		currentZeroStamp = GetDateTime(currentTime)
		day := time.Now().Day()
		fmt.Println("day:", day)
		if day != tempDay {
			// 插入
			startTime := strconv.FormatInt(currentZeroStamp-48*3600, 10)
			endTime := strconv.FormatInt(currentZeroStamp, 10)

			resp := httpClient(startTime, endTime, dataPicker.channelConfig.Iotmn)

			fmt.Println("开始时间:", startTime)
			fmt.Println("结束时间:", endTime)

			lens := len(resp.InfraredData)

			var u1 *InfraredData
			var u2 *InfraredData

			if lens > 0 {
				if lens == 12 {
					for i := range resp.InfraredData {
						if i == 0 {
							u1 = &resp.InfraredData[i]
						}
						if i == 6 {
							u2 = &resp.InfraredData[i]
						}
					}
				} else if lens == 24 {
					for i := range resp.InfraredData {
						if i == 0 {
							u1 = &resp.InfraredData[i]
						}
						if i == 12 {
							u2 = &resp.InfraredData[i]
						}
					}
				}

				temp := fmt.Sprintf("%.4f", u1.FAEERtd-u2.FAEERtd)

				dataPicker.infraredData.FAEERtd, _ = strconv.ParseFloat(temp, 64)
				dataPicker.infraredData.EemeidRtd = u1.EemeidRtd
				fmt.Println("电表消耗量:", dataPicker.infraredData.FAEERtd)
				err := InsertDb(dataPicker, startTime, endTime)
				if err != nil {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn}).Error("insert error:", err)
				} else {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn}).Info("insert success")
				}

			} else {
				time.Sleep(10 * time.Second)
				continue
			}

			// 赋值tempDay
			tempDay = day
		} else {
			fmt.Println("今天已经插入过数据了")
		}

		time.Sleep(10 * time.Second)
	}

}

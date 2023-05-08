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
	TransRatio   float64  `json:"transRatio"`
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

func GetDateTime(date string, layout string) int64 {

	loc, _ := time.LoadLocation("Local")
	zeroTimeStamp, _ := time.ParseInLocation(layout, date, loc)

	return zeroTimeStamp.Unix()
}

func GetMonthZeroStamp(month time.Month) int64 {
	now := time.Now()
	lastMonth := time.Date(now.Year(), month, 1, 0, 0, 0, 0, time.Local)
	return lastMonth.Unix()
}

func GetInfoAndInsert(dataPicker *DataPickerStFctsdb) {

	//time.Sleep(3 * time.Second)

	var currentTime string
	var currentZeroStamp int64

	var iotCodeValues []float64
	var faeeRtdArr []float64
	var iaeeRtdArr []float64

	var faeeRtdSum float64
	var iaeeRtdSum float64
	var sumArr []float64

	var tempDay = 0

	sumIotCode := []string{"FAEE-Sum", "IAEE-Sum"}

	for {
		currentTime = time.Now().Format("2006-01-02")
		// 当天零点时间戳
		currentZeroStamp = GetDateTime(currentTime, "2006-01-02")
		day := time.Now().Day()
		month := time.Now().Month()

		fmt.Println("day:", day)
		if day != tempDay {

			// 进行月统计（每月1日）
			if day == 1 {
				for _, item := range faeeRtdArr {
					faeeRtdSum += item
				}
				for _, item := range iaeeRtdArr {
					iaeeRtdSum += item
				}

				sumArr = append(sumArr, faeeRtdSum, iaeeRtdSum)
				for i := 0; i < len(dataPicker.channelConfig.Iotcode); i++ {
					err := InsertDb(dataPicker,
						GetMonthZeroStamp(month-1),
						GetMonthZeroStamp(month),
						sumIotCode[i],
						sumArr[i])
					if err != nil {
						logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn}).Error("insert error:", err)
					} else {
						logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn}).Info("insert success")
					}
				}

				// 释放所有数组元素
				faeeRtdArr = faeeRtdArr[:0]
				faeeRtdArr = nil
				iaeeRtdArr = iaeeRtdArr[:0]
				iaeeRtdArr = nil
				iotCodeValues = iotCodeValues[:0]
				iotCodeValues = nil
				sumArr = sumArr[:0]
				sumArr = nil
			}
			// 插入
			beforeYesterdayTime := strconv.FormatInt(currentZeroStamp-48*3600, 10)
			yesterdayTime := strconv.FormatInt(currentZeroStamp-24*3600, 10)
			todayTime := strconv.FormatInt(currentZeroStamp, 10)

			resp1 := httpClient(yesterdayTime, todayTime, dataPicker.channelConfig.Iotmn)
			resp2 := httpClient(beforeYesterdayTime, yesterdayTime, dataPicker.channelConfig.Iotmn)

			yesterdayObj := resp1.InfraredData[0]
			beforeYesterdayObj := resp2.InfraredData[0]

			// 判断正向有功总电能是否为无效值
			//	if u1.FAEERtd != -100 && u2.FAEERtd != -100 {
			temp1 := fmt.Sprintf("%.4f", (yesterdayObj.FAEERtd-beforeYesterdayObj.FAEERtd)*dataPicker.channelConfig.TransRatio) // 2023-05-05 add transRatio
			dataPicker.infraredData.FAEERtd, _ = strconv.ParseFloat(temp1, 64)
			dataPicker.infraredData.EemeidRtd = yesterdayObj.EemeidRtd
			faeeRtdArr = append(faeeRtdArr, dataPicker.infraredData.FAEERtd)
			logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn, "FAEERtd": dataPicker.infraredData.FAEERtd})
			fmt.Println("电表消耗量(正向有功总电能):", dataPicker.infraredData.FAEERtd)
			//	}

			// 判断反向有功总电能是否为无效值
			//	if u1.IAEERtd != -100 && u2.IAEERtd != -100 {
			temp2 := fmt.Sprintf("%.4f", (yesterdayObj.IAEERtd-beforeYesterdayObj.IAEERtd)*dataPicker.channelConfig.TransRatio) // 2023-05-05 add transRatio
			dataPicker.infraredData.IAEERtd, _ = strconv.ParseFloat(temp2, 64)
			dataPicker.infraredData.EemeidRtd = yesterdayObj.EemeidRtd
			iaeeRtdArr = append(iaeeRtdArr, dataPicker.infraredData.IAEERtd)
			logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn, "IAEERtd": dataPicker.infraredData.IAEERtd})
			fmt.Println("电表消耗量(反向有功总电能):", dataPicker.infraredData.IAEERtd)
			//	}
			iotCodeValues = append(iotCodeValues, dataPicker.infraredData.FAEERtd, dataPicker.infraredData.IAEERtd)
			for i := 0; i < len(dataPicker.channelConfig.Iotcode); i++ {
				err := InsertDb(dataPicker,
					GetDateTime(beforeYesterdayObj.DataTime, "20060102150405"),
					GetDateTime(yesterdayObj.DataTime, "20060102150405"),
					dataPicker.channelConfig.Iotcode[i],
					iotCodeValues[i])
				if err != nil {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn}).Error("insert error:", err)
				} else {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.channelConfig.Iotmn}).Info("insert success")
				}
			}

			//清空iotCodeValues数组，第二天重新写入
			iotCodeValues = iotCodeValues[:0]
			iotCodeValues = nil

			// 赋值tempDay
			tempDay = day

		} else {
			fmt.Println("今天已经插入过数据了")
		}

		time.Sleep(10 * time.Second)
	}

}

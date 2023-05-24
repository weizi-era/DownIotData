package main

import (
	"DownIotData/db"
	"DownIotData/http"
	"DownIotData/model"
	"DownIotData/service"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 解析命令行参数
func initFlag(pAppConfig *model.AppConfigSt) {
	flag.StringVar(&pAppConfig.ConfigDir, "configdir", "./etc/", "set config file dir")
	flag.StringVar(&pAppConfig.LogDir, "logdir", "./log/", "set log file dir")
	flag.IntVar(&pAppConfig.LogLevel, "loglevel", 4, "set log level 0-6 panic fatal error warn info debug trace")

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

func initSysConfig(Configdir string, pConfig *model.ConfigParams) {

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

	var ratioArr []float64

	initFlag(&model.AppConfig)
	if (len(model.AppConfig.ConfigDir) > 0) && (model.AppConfig.ConfigDir[len(model.AppConfig.ConfigDir)-1] == ':') {
		model.AppConfig.ConfigDir += "/etc/"
	}

	initLog(model.AppConfig.LogDir, model.AppConfig.LogLevel)

	initSysConfig(model.AppConfig.ConfigDir, &model.SysConfig)

	logrus.WithFields(logrus.Fields{"sysconfig": model.SysConfig}).Info("initSysconfig")

	for _, v := range model.SysConfig.IotConfig {
		ratio := http.GetTransRatio(v.Iotmn)
		ratioF, _ := strconv.ParseFloat(ratio.Value, 64)
		ratioArr = append(ratioArr, ratioF)
	}

	fmt.Println(ratioArr)

	for i, v := range model.SysConfig.IotConfig {
		if len(v.Iotmn) == 0 {
			logrus.WithFields(logrus.Fields{"configindex": i, "hjt212config": v}).
				Error("hjt212config mn slice size != code slice size")
		}

		if strings.Contains(model.SysConfig.DbConfig.Localdburl, "8900") {
			var dataPicker db.DataPickerStFctsdb
			dataPicker.Id = i
			dataPicker.ChannelConfig = &model.SysConfig.IotConfig[i]
			dataPicker.DbConfig = &model.SysConfig.DbConfig

			go service.GetInfoAndInsertFctsdb(&dataPicker, ratioArr[i])
		} else if strings.Contains(model.SysConfig.DbConfig.Localdburl, "3306") {
			var dataPicker db.DataPickerStMySQL
			dataPicker.Id = i
			dataPicker.ChannelConfig = &model.SysConfig.IotConfig[i]
			dataPicker.DbConfig = &model.SysConfig.DbConfig
			go service.GetInfoAndInsertMySQL(&dataPicker, ratioArr[i])
		}

		fmt.Println("协程结束")
	}

	for {
		time.Sleep(10 * time.Second)
	}
}

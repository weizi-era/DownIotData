package service

import (
	"DownIotData/db"
	"DownIotData/http"
	"DownIotData/utils"
	"fmt"
	"github.com/sirupsen/logrus"
	"strconv"
	"time"
)

func GetInfoAndInsertFctsdb(dataPicker *db.DataPickerStFctsdb, ratio float64) {

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
		currentZeroStamp = utils.GetDateTime(currentTime, "2006-01-02").Unix()
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
				for i := 0; i < len(dataPicker.ChannelConfig.Iotcode); i++ {
					err := dataPicker.InsertDb(
						utils.GetLastMonthZero(month-1).Unix(),
						utils.GetLastMonthZero(month).Unix(),
						sumIotCode[i],
						sumArr[i])
					if err != nil {
						logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Error("insert error:", err)
					} else {
						logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Info("insert success")
					}

					time.Sleep(500 * time.Millisecond)
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

			resp1 := http.HttpClient(yesterdayTime, todayTime, dataPicker.ChannelConfig.Iotmn)
			resp2 := http.HttpClient(beforeYesterdayTime, yesterdayTime, dataPicker.ChannelConfig.Iotmn)

			yesterdayObj := resp1.InfraredData[0]
			beforeYesterdayObj := resp2.InfraredData[0]

			// 判断正向有功总电能是否为无效值
			//	if u1.FAEERtd != -100 && u2.FAEERtd != -100 {
			temp1 := fmt.Sprintf("%.4f", (yesterdayObj.FAEERtd-beforeYesterdayObj.FAEERtd)*ratio) // 2023-05-05 add transRatio
			dataPicker.InfraredData.FAEERtd, _ = strconv.ParseFloat(temp1, 64)
			dataPicker.InfraredData.EemeidRtd = yesterdayObj.EemeidRtd
			faeeRtdArr = append(faeeRtdArr, dataPicker.InfraredData.FAEERtd)
			logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn, "FAEERtd": dataPicker.InfraredData.FAEERtd})
			fmt.Println("电表消耗量(正向有功总电能):", dataPicker.InfraredData.FAEERtd)
			//	}

			// 判断反向有功总电能是否为无效值
			//	if u1.IAEERtd != -100 && u2.IAEERtd != -100 {
			temp2 := fmt.Sprintf("%.4f", (yesterdayObj.IAEERtd-beforeYesterdayObj.IAEERtd)*ratio) // 2023-05-05 add transRatio
			dataPicker.InfraredData.IAEERtd, _ = strconv.ParseFloat(temp2, 64)
			dataPicker.InfraredData.EemeidRtd = yesterdayObj.EemeidRtd
			iaeeRtdArr = append(iaeeRtdArr, dataPicker.InfraredData.IAEERtd)
			logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn, "IAEERtd": dataPicker.InfraredData.IAEERtd})
			fmt.Println("电表消耗量(反向有功总电能):", dataPicker.InfraredData.IAEERtd)
			//	}
			iotCodeValues = append(iotCodeValues, dataPicker.InfraredData.FAEERtd, dataPicker.InfraredData.IAEERtd)
			for i := 0; i < len(dataPicker.ChannelConfig.Iotcode); i++ {
				err := dataPicker.InsertDb(
					utils.GetDateTime(beforeYesterdayObj.DataTime, "20060102150405").Unix(),
					utils.GetDateTime(yesterdayObj.DataTime, "20060102150405").Unix(),
					dataPicker.ChannelConfig.Iotcode[i],
					iotCodeValues[i])
				if err != nil {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Error("insert error:", err)
				} else {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Info("insert success")
				}
				time.Sleep(500 * time.Millisecond)
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

func GetInfoAndInsertMySQL(dataPicker *db.DataPickerStMySQL, ratio float64) {

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
		currentZeroStamp = utils.GetDateTime(currentTime, "2006-01-02").Unix()
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
				for i := 0; i < len(dataPicker.ChannelConfig.Iotcode); i++ {
					err := dataPicker.InsertDb(
						utils.GetLastMonthZero(month-1),
						utils.GetLastMonthZero(month),
						sumIotCode[i],
						sumArr[i])
					if err != nil {
						logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Error("insert error:", err)
					} else {
						logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Info("insert success")
					}

					time.Sleep(500 * time.Millisecond)
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

			resp1 := http.HttpClient(yesterdayTime, todayTime, dataPicker.ChannelConfig.Iotmn)
			resp2 := http.HttpClient(beforeYesterdayTime, yesterdayTime, dataPicker.ChannelConfig.Iotmn)

			yesterdayObj := resp1.InfraredData[0]
			beforeYesterdayObj := resp2.InfraredData[0]

			// 判断正向有功总电能是否为无效值
			//	if u1.FAEERtd != -100 && u2.FAEERtd != -100 {
			temp1 := fmt.Sprintf("%.4f", (yesterdayObj.FAEERtd-beforeYesterdayObj.FAEERtd)*ratio) // 2023-05-05 add transRatio
			dataPicker.InfraredData.FAEERtd, _ = strconv.ParseFloat(temp1, 64)
			dataPicker.InfraredData.EemeidRtd = yesterdayObj.EemeidRtd
			faeeRtdArr = append(faeeRtdArr, dataPicker.InfraredData.FAEERtd)
			logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn, "FAEERtd": dataPicker.InfraredData.FAEERtd})
			fmt.Println("电表消耗量(正向有功总电能):", dataPicker.InfraredData.FAEERtd)
			//	}

			// 判断反向有功总电能是否为无效值
			//	if u1.IAEERtd != -100 && u2.IAEERtd != -100 {
			temp2 := fmt.Sprintf("%.4f", (yesterdayObj.IAEERtd-beforeYesterdayObj.IAEERtd)*ratio) // 2023-05-05 add transRatio
			dataPicker.InfraredData.IAEERtd, _ = strconv.ParseFloat(temp2, 64)
			dataPicker.InfraredData.EemeidRtd = yesterdayObj.EemeidRtd
			iaeeRtdArr = append(iaeeRtdArr, dataPicker.InfraredData.IAEERtd)
			logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn, "IAEERtd": dataPicker.InfraredData.IAEERtd})
			fmt.Println("电表消耗量(反向有功总电能):", dataPicker.InfraredData.IAEERtd)
			//	}
			iotCodeValues = append(iotCodeValues, dataPicker.InfraredData.FAEERtd, dataPicker.InfraredData.IAEERtd)
			for i := 0; i < len(dataPicker.ChannelConfig.Iotcode); i++ {
				err := dataPicker.InsertDb(
					utils.GetDateTime(beforeYesterdayObj.DataTime, "20060102150405"),
					utils.GetDateTime(yesterdayObj.DataTime, "20060102150405"),
					dataPicker.ChannelConfig.Iotcode[i],
					iotCodeValues[i])
				if err != nil {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Error("insert error:", err)
				} else {
					logrus.WithFields(logrus.Fields{"mn": dataPicker.ChannelConfig.Iotmn}).Info("insert success")
				}
				time.Sleep(500 * time.Millisecond)
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

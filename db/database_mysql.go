package db

import (
	"DownIotData/http"
	"DownIotData/model"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

///////////////////////////////////////////////////////////////////////////
// 使用MySQL数据库作为数据源的数据查询与数据更新模块

type RkdataQueryMySQLInfo struct {
	Id         string
	CarbonId   string
	DeviceId   string
	DeviceType string
	Flag       string
	MeterId    string
	MeterType  string
	Rkkey      string
	StatType   string
	Dvalue     float64
	CreateTime time.Time
	BeginTime  time.Time
	EndTime    time.Time
	Upload1    uint32
	Upload2    uint32
	Upload3    uint32
	Valid      uint32
}

type DataPickerStMySQL struct {
	Id            int
	InfraredData  *http.InfraredData
	ChannelConfig *model.IotConfig
	DbConfig      *model.DbConfig
	gormDB        *gorm.DB
}

func (r *RkdataQueryMySQLInfo) TableName() string {
	return "rkdata"
}

// InitGormDB 初始数据gorm实例
func (p *DataPickerStMySQL) InitGormDB() error {
	if p.gormDB != nil {
		return nil
	}

	db, err := gorm.Open(mysql.Open(p.DbConfig.Localdburl), &gorm.Config{})
	if err != nil {
		logrus.WithFields(logrus.Fields{"err": err}).Error("DataPickerStMySQL init gorm db error")
		return err
	} else {
		logrus.Info("DataPickerStMySQL init gorm db success")
		p.gormDB = db
	}

	return nil
}

// InsertDb  插入数据
func (p *DataPickerStMySQL) InsertDb(startTime time.Time, endTime time.Time, iotCode string, value float64) error {

	//start, _ := strconv.ParseInt(startTime, 10, 64)
	//end, _ := strconv.ParseInt(endTime, 10, 64)

	if p.gormDB == nil {
		err := p.InitGormDB()
		if err != nil {
			return err
		}
	}

	{
		var newInfo RkdataQueryMySQLInfo
		newInfo.CarbonId = p.DbConfig.CarbonId
		newInfo.Flag = p.DbConfig.Flag
		newInfo.DeviceType = p.DbConfig.DeviceType
		newInfo.MeterId = p.DbConfig.MeterId
		newInfo.MeterType = p.DbConfig.MeterType
		newInfo.StatType = p.DbConfig.StatType
		newInfo.Rkkey = iotCode
		newInfo.Dvalue = value / 1000
		newInfo.DeviceId = p.InfraredData.EemeidRtd
		newInfo.CreateTime = time.Now().Local().Truncate(time.Second)
		newInfo.BeginTime = startTime
		newInfo.EndTime = endTime
		newInfo.Upload1 = 1
		newInfo.Upload2 = 1
		newInfo.Upload3 = 1
		newInfo.Valid = 0
		newInfo.Id = uuid.New().String()

		p.gormDB = p.gormDB.Create(&newInfo)
		if p.gormDB.Error != nil {
			logrus.WithFields(logrus.Fields{"err": p.gormDB.Error}).Error("DataPickerStMySQL insert db error")
			return p.gormDB.Error
		}
	}

	return nil
}

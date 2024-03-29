package db

import (
	"DownIotData/http"
	"DownIotData/model"
	"database/sql/driver"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

///////////////////////////////////////////////////////////////////////////
// 使用海东青数据库作为数据源的数据查询与数据更新模块

type LocalTime time.Time

func (t LocalTime) Value() (driver.Value, error) {
	var zeroTime time.Time
	tlt := time.Time(t)
	//判断给定时间是否和默认零时间的时间戳相同
	if tlt.UnixNano() == zeroTime.UnixNano() {
		return nil, nil
	}
	return tlt, nil
}

func (t *LocalTime) Scan(v interface{}) error {
	if _, ok := v.([]byte); ok {
		//v, err := time.Parse("2006-01-02 15:04:05", string(v.([]byte)))
		v, err := time.ParseInLocation("2006-01-02 15:04:05", string(v.([]byte)), time.Local)
		if err != nil {
			return err
		}
		*t = LocalTime(v)
		return nil
	}
	return fmt.Errorf("can not convert %v to timestamp", v)
}

type DataSrcIf interface {
	InsertDb() ([]RkdataQueryFctsdbInfo, error)
}

type RkdataQueryFctsdbInfo struct {
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
	CreateTime int64
	BeginTime  int64
	EndTime    int64
	Upload1    uint32
	Upload2    uint32
	Upload3    uint32
	Valid      uint32
	Time       time.Time
}

func (r *RkdataQueryFctsdbInfo) TableName() string {
	return "rkdata"
}

type DataPickerStFctsdb struct {
	Id            int
	InfraredData  http.InfraredData
	ChannelConfig *model.IotConfig
	DbConfig      *model.DbConfig
	gormDB        *gorm.DB
}

// InsertDb  插入数据
func (p *DataPickerStFctsdb) InsertDb(startTime int64, endTime int64, iotCode string, value float64) error {

	//start, _ := strconv.ParseInt(startTime, 10, 64)
	//end, _ := strconv.ParseInt(endTime, 10, 64)

	if p.gormDB == nil {
		err := p.InitGormDB()
		if err != nil {
			return err
		}
	}

	{
		var newInfo RkdataQueryFctsdbInfo
		newInfo.CarbonId = p.DbConfig.CarbonId
		newInfo.Flag = p.DbConfig.Flag
		newInfo.DeviceType = p.DbConfig.DeviceType
		newInfo.MeterId = p.DbConfig.MeterId
		newInfo.MeterType = p.DbConfig.MeterType
		newInfo.StatType = p.DbConfig.StatType
		newInfo.Rkkey = iotCode
		newInfo.Dvalue = value / 1000
		newInfo.DeviceId = p.InfraredData.EemeidRtd
		newInfo.CreateTime = time.Now().UnixNano()
		newInfo.BeginTime = startTime * 1e9
		newInfo.EndTime = endTime * 1e9
		newInfo.Upload1 = 1
		newInfo.Upload2 = 1
		newInfo.Upload3 = 1
		newInfo.Valid = 0
		newInfo.Id = uuid.New().String()
		newInfo.Time = time.Now().UTC().Truncate(time.Second)

		p.gormDB = p.gormDB.Create(&newInfo)
		if p.gormDB.Error != nil {
			logrus.WithFields(logrus.Fields{"err": p.gormDB.Error}).Error("DataPickerStFctsdb insert db error")
			return p.gormDB.Error
		}
	}

	return nil
}

// QueryDb 查询数据
func (p *DataPickerStFctsdb) QueryDb() ([]RkdataQueryFctsdbInfo, error) {

	if p.gormDB == nil {
		err := p.InitGormDB()
		if err != nil {
			return nil, err
		}
	}

	var info []RkdataQueryFctsdbInfo
	var result *gorm.DB

	result = p.gormDB.Where("rkkey=?", p.ChannelConfig.Name).Find(&info)
	if result.Error != nil {
		logrus.WithFields(logrus.Fields{"err": result.Error}).Error("DataPickerStFctsdb query db error")
		return nil, result.Error
	}

	return info, nil

}

// InitGormDB 初始数据gorm实例
func (p *DataPickerStFctsdb) InitGormDB() error {
	if p.gormDB != nil {
		return nil
	}

	db, err := gorm.Open(mysql.Open(p.DbConfig.Localdburl), &gorm.Config{})
	if err != nil {
		logrus.WithFields(logrus.Fields{"err": err}).Error("DataPickerStFctsdb init gorm db error")
		return err
	} else {
		logrus.Info("DataPickerStFctsdb init gorm db success")
		p.gormDB = db
	}

	return nil
}

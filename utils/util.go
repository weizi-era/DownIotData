package utils

import "time"

func GetDateTime(date string, layout string) time.Time {

	loc, _ := time.LoadLocation("Local")
	times, _ := time.ParseInLocation(layout, date, loc)

	return times
}

func GetLastMonthZero(month time.Month) time.Time {
	now := time.Now()
	lastMonth := time.Date(now.Year(), month, 1, 0, 0, 0, 0, time.Local)
	return lastMonth
}

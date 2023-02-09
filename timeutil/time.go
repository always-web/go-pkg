package timeutil

import "time"

var cst *time.Location

// CSTLayout yyyy-MM-dd HH:mm:ss 格式
const CSTLayout = "2006-01-02 15:04:05"

// YMDLayout yyyyMMdd 格式
const YMDLayout = "20060102"

func init() {
	var err error
	if cst, err = time.LoadLocation("Asia/Shanghai"); err != nil {
		panic(err)
	}
	time.Local = cst
}

// RFC3339ToCSTLayout 将 RFC3339 格式转换成 yyyy-MM-dd HH:mm:ss 格式
// 2006-01-02T15:04:05Z07:00 ==> 2006-01-02 15:04:05
func RFC3339ToCSTLayout(value string) (string, error) {
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", err
	}
	return ts.In(cst).Format(CSTLayout), nil
}

// ParseCSTInLocation yyyy-MM-dd HH:mm:ss 格式转换成时间类型
func ParseCSTInLocation(date string) (time.Time, error) {
	return time.ParseInLocation(CSTLayout, date, cst)
}

// CSTLayoutString 返回 yyyy-MM-dd HH:mm:ss 格式的当前时间
func CSTLayoutString() string {
	ts := time.Now()
	return ts.In(cst).Format(CSTLayout)
}

// CSTLayoutStringToUnix yyyy-MM-dd HH:mm:ss 格式字符串转换成 uninx 时间戳格式
// 2020-01-24 21:11:11 => 1579871471
func CSTLayoutStringToUnix(cstLayoutStr string) (int64, error) {
	stamp, err := time.ParseInLocation(CSTLayout, cstLayoutStr, cst)
	if err != nil {
		return 0, err
	}
	return stamp.Unix(), nil
}

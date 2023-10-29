package logger

import (
	"github.com/pkg/errors"
	"testing"
)

func TestJSONLogger(t *testing.T) {
	logger := InitLogger(
		WithField("default_key", "default_value"),
	)
	defer logger.Sync()
	err := errors.New("pkg error")
	logger.Error("err occurs", WrapMeta(nil, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
	logger.Error("err occurs", WrapMeta(err, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
}

func TestFileLogger(t *testing.T) {
	logger := InitLogger(WithFileP("log.log"))
	defer logger.Sync()
	err := errors.New("pkg error")
	logger.Error("err occurs", WrapMeta(nil, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
	logger.Error("err occurs", WrapMeta(err, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
}

func TestRotationFileLogger(t *testing.T) {
	logger := InitLogger(WithFileRotationP("log.log"))
	defer logger.Sync()
	err := errors.New("pkg error")
	for true {
		logger.Error("err occurs", WrapMeta(nil, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
		logger.Error("err occurs", WrapMeta(err, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
	}
}

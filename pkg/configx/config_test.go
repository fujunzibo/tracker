package configx

import (
	"errors"
	"reflect"
	"testing"
)

type testConfig struct {
	AppName string
	Users   []UserConfig
	Log     *LogConfig
}

func (t *testConfig) Preset() {
	t.AppName = "Test Config"
	t.Log = &LogConfig{}
	t.Users = []UserConfig{{Account: "zs", Name: "张三"}}
}

type LogConfig struct {
	Path    string
	Level   string
	Console bool
}

func (t *LogConfig) Preset() {
	t.Path = "logs"
}

func (t LogConfig) Validate() error {
	if t.Path == "" {
		return errors.New("log path 不能为空")
	}
	if t.Level == "" {
		return errors.New("log level 不能为空")
	}
	return nil
}

type UserConfig struct {
	Name    string
	Account string
	Age     int
}

func (t *UserConfig) Preset() {
	t.Age = 16
}

func (t *UserConfig) Validate() error {
	if t.Account == "" {
		return errors.New("account 不能为空")
	}
	if t.Name == "" {
		return errors.New("name 不能为空")
	}
	if t.Age < 18 {
		return errors.New("age 不能小于18")
	}
	return nil
}

func TestPreset(t *testing.T) {
	c := &testConfig{}
	v := reflect.ValueOf(c)
	Preset(v)
	t.Log(c.AppName)
	t.Log(c.Log)
	t.Log(c.Users)
}

func TestValidate(t *testing.T) {
	c := &testConfig{}
	v := reflect.ValueOf(c)
	Preset(v)
	err := Validate(v)
	if err == nil {
		t.FailNow()
	}
	t.Log(err)
}

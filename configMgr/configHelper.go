package configMgr

import (
	"github.com/spf13/viper"
	"strings"
	"sync"
)

type EventConfig struct {
	*viper.Viper
	logLevel map[string]string
	sync.RWMutex
	Config *Config
}
type Config struct {
	Ip                string
	Port              string
	Username          string
	Passwd            string
	DataBaseName      string
	RegisterTableName string
	EventmsgtableName string
	IdleConn          int
	MaxConn           int
	LibName           string
	Mport             string
}

/**
* @Title: NewEventConfig.go
* @Description: NewEventConfig  获取config工具
* @author ghc
* @date 9/25/18 13:42 PM
* @version V1.0
 */
func (evs *EventConfig) NewEventConfig() (econfig *EventConfig, err error) {

	config := viper.New()
	config.AddConfigPath("./configFile/")
	config.AddConfigPath("../configFile/")
	config.AddConfigPath("../../configFile/")
	config.SetConfigName("eventConfig") // name of config file (without extension)
	config.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)
	errRd := config.ReadInConfig()
	if err != nil {
		return nil, errRd
	}
	ec := &EventConfig{}
	ec.Viper = config
	ec.logLevel = make(map[string]string)

	ip := ec.GetString("mysql.ip")
	port := ec.GetString("mysql.port")
	username := ec.GetString("mysql.username")
	passwd := ec.GetString("mysql.passwd")
	dataBaseName := ec.GetString("mysql.dataBaseName")
	registerTableName := ec.GetString("mysql.registerTableName")
	eventmsgtableName := ec.GetString("mysql.eventmsgtableName")
	idleConn := ec.GetInt("mysql.idleConn")
	maxConn := ec.GetInt("mysql.maxConn")
	libName := ec.GetString("mysql.libName")
	mport := ec.GetString("monitor.mport")
	conf := &Config{
		Ip:                ip,
		Port:              port,
		Username:          username,
		Passwd:            passwd,
		DataBaseName:      dataBaseName,
		RegisterTableName: registerTableName,
		EventmsgtableName: eventmsgtableName,
		IdleConn:          idleConn,
		MaxConn:           maxConn,
		LibName:           libName,
		Mport:             mport,
	}
	ec.Config = conf
	return ec, nil

}

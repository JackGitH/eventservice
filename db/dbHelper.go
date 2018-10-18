package db

import (
	"database/sql"
	"eventservice/configMgr"
	"fmt"
	_ "github.com/go-sql-driver/MySQL"
	"github.com/op/go-logging"
	"io/ioutil"
	"runtime"
)

type DbHandler struct {
	Db *sql.DB
}

var Port string

var registerDbLog = logging.MustGetLogger("registerDb")

/**
* @Title: reistgetDbHandlererDb.go
* @Description: getDbHandler  获取db句柄
* @author ghc
* @date 9/25/18 13:42 PM
* @version V1.0
 */
func (dh *DbHandler) GetDbHandler() (db *DbHandler, err error) {
	ev := &configMgr.EventConfig{}
	ev, err = ev.NewEventConfig()
	if err != nil {
		return nil, err
	}
	ip := ev.Config.Ip
	port := ev.Config.Port
	userName := ev.Config.Username
	passwd := ev.Config.Passwd
	dataBaseName := ev.Config.DataBaseName
	mport := ev.Config.Mport
	Port = ":" + mport

	dbh, err := sql.Open("mysql", userName+":"+passwd+"@tcp("+ip+":"+port+")/"+"?charset=utf8&multiStatements=true")

	//dbh.Ping()

	if err != nil {
		fmt.Print("err", err)
		return nil, err
	}
	//建库
	// todo linux 下目录使用 docs/database/createDataBase.sql
	sys := string(runtime.GOOS) // 判断操作系统
	var sqlBytes []byte
	if sys == "windows" {
		sqlBytes, err = ioutil.ReadFile("docs/database/createDataBase.sql")
	} else {
		sqlBytes, err = ioutil.ReadFile("../docs/database/createDataBase.sql")
	}
	if err != nil {
		registerDbLog.Error("ioutil.ReadFile createDataBase.sql err", err)
		fmt.Print("ioutil.ReadFile createDataBase.sql fail")
		return
	}
	sqlDataBase := string(sqlBytes)
	fmt.Println("sqlTable", sqlDataBase)

	result, err := dbh.Exec(sqlDataBase)
	if err != nil {
		registerDbLog.Error("dbh.Exec sqlDataBase err", err, result)
		fmt.Print("dbh.Exec sqlDataBase fail")
		return
	}
	registerDbLog.Info("createDataBase success")
	fmt.Println("createDataBase success")
	//dataBaseName 要与sql语句名称一致
	dbhBase, err := sql.Open("mysql", userName+":"+passwd+"@tcp("+ip+":"+port+")/"+dataBaseName+"?charset=utf8&multiStatements=true")
	dbhBase.SetConnMaxLifetime(0)
	dbhBase.SetMaxOpenConns(50) // 最大连接数
	dbhBase.SetMaxIdleConns(50) // 空闲连接数
	if err != nil {
		registerDbLog.Error("sql.Open dbhBase err", err)
		fmt.Print("sql.Open dbhBase err fail")
		return
	}
	dh.Db = dbhBase
	return dh, nil
}

//test
/*func main() {
	test := &DbHandler{}
	test.GetDbHandler()
}*/

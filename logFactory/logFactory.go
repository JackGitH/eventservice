/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package log

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

// A logger to log logging logs!
var Config *viper.Viper
var callback func(logFile string, config *viper.Viper)

var loggingLogger = logging.MustGetLogger("logging")

// The default logging level, in force until LoggingInit() is called or in
// case of configuration errors.
var loggingDefaultLevel = logging.INFO

const (
	_VER string = "1.0.2"
)
const (
	NETWORK           string = "network"
	LEDGER            string = "ledger"
	REST              string = "rest"
	MINEPOOL          string = "minPool"
	WORKER            string = "worker"
	DIFFICULTYMANAGER string = "difficultyManager"
	CHAINSMANAGER     string = "chainsManager"
	QUEUE             string = "queue"
	POW               string = "PoW"
	SYNC              string = "sync"
	ORIGINALBATCHS    string = "originalbatchs"
	TXMANAGER         string = "txManager"
	SECURITY          string = "security"
	BLOCKINFO         string = "blockInfo"
	FORKINFO          string = "forkInfo"
	APP               string = "app"
	UIOC              string = "uioc"
)

type LEVEL int32

var logLevel LEVEL = 1
var maxFileSize int64
var maxFileCount int32
var dailyRolling bool = true
var consoleAppender bool = true
var RollingFile bool = false
var logObj *_FILE

const DATEFORMAT = "2006-01-02 15"
const RENAMEDATEFORMAT = "2006-01-02_15"

type UNIT int64

const (
	_       = iota
	KB UNIT = 1 << (iota * 10)
	MB
	GB
	TB
)

const (
	ALL LEVEL = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
	OFF
)

type _FILE struct {
	dir      string
	filename string
	_suffix  int
	isCover  bool
	_date    *time.Time
	mu       *sync.RWMutex
	logfile  *os.File
	lg       *log.Logger
}

func SetConsole(isConsole bool) {
	consoleAppender = isConsole
}

func SetLevel(_level LEVEL) {
	logLevel = _level
}

//指定日志文件备份方式为文件大小的方式
//第一个参数为日志文件存放目录
//第二个参数为日志文件命名
//第三个参数为备份文件最大数量
//第四个参数为备份文件大小
//第五个参数为文件大小的单位
//SetRollingFile("d:/logtest", "test.log", 10, 5, logger.KB)
func SetRollingFile(fileDir, fileName string, maxNumber int32, maxSize int64, _unit UNIT) {
	maxFileCount = maxNumber
	maxFileSize = maxSize * int64(_unit)
	RollingFile = true
	dailyRolling = false
	Mkdirlog(fileDir)
	logObj = &_FILE{dir: fileDir, filename: fileName, isCover: false, mu: new(sync.RWMutex)}
	logObj.mu.Lock()
	defer logObj.mu.Unlock()
	for i := 0; i <= int(maxNumber); i++ {
		if isExist(fileDir + "/" + fileName + "." + strconv.Itoa(i)) {
			logObj._suffix = i
		} else {
			break
		}
	}

	if !logObj.isMustRename() {
		logObj.logfile, _ = os.OpenFile(fileDir+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		logObj.lg = log.New(logObj.logfile, "", log.Ldate|log.Ltime|log.Lshortfile)
		logObj.filename = fileName
	} else {
		logObj.rename()
	}
	go fileMonitor()
}

//指定日志文件备份方式为日期的方式
//第一个参数为日志文件存放目录
//第二个参数为日志文件命名
//SetRollingDaily("d:/logtest", "test.log")
func SetRollingDaily(fileDir, fileName string, config *viper.Viper, function func(logFile string, config *viper.Viper)) {
	RollingFile = false
	dailyRolling = true
	Config = config
	callback = function
	t, _ := time.Parse(DATEFORMAT, time.Now().Format(DATEFORMAT))
	Mkdirlog(fileDir)
	logObj = &_FILE{dir: fileDir, filename: fileName, _date: &t, isCover: false, mu: new(sync.RWMutex)}
	logObj.mu.Lock()
	defer logObj.mu.Unlock()
	if !logObj.isMustRename() {
		logObj.logfile, _ = os.OpenFile(fileDir+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		logObj.lg = log.New(logObj.logfile, "", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		logObj.rename()
	}
	go fileMonitor()
}

func Mkdirlog(dir string) (e error) {
	_, er := os.Stat(dir)
	b := er == nil || os.IsExist(er)
	if !b {
		if err := os.MkdirAll(dir, 0777); err != nil {
			if os.IsPermission(err) {
				fmt.Println("create dir error:", err.Error())
				e = err
			}
		}
	}
	return
}

func console(s ...interface{}) {
	if consoleAppender {
		_, file, line, _ := runtime.Caller(2)
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		log.Println(file, strconv.Itoa(line), s)
	}
}

func catchError() {
	if err := recover(); err != nil {
		log.Println("err", err)
	}
}

func Debug(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= DEBUG {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintln("debug", v))
		}
		console("debug", v)
	}
}
func Info(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= INFO {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintln("info", v))
		}
		console("info", v)
	}
}
func Warn(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}

	if logLevel <= WARN {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintln("warn", v))
		}
		console("warn", v)
	}
}
func Error(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= ERROR {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintln("error", v))
		}
		console("error", v)
	}
}
func Fatal(v ...interface{}) {
	if dailyRolling {
		fileCheck()
	}
	defer catchError()
	if logObj != nil {
		logObj.mu.RLock()
		defer logObj.mu.RUnlock()
	}
	if logLevel <= FATAL {
		if logObj != nil {
			logObj.lg.Output(2, fmt.Sprintln("fatal", v))
		}
		console("fatal", v)
	}
}

func (f *_FILE) isMustRename() bool {
	if dailyRolling {
		t, _ := time.Parse(DATEFORMAT, time.Now().Format(DATEFORMAT))
		//fmt.Println("now: ",time.Now().Format(DATEFORMAT)," t: ",t," t.After(*f._date): ",t.After(*f._date))
		if t.After(*f._date) {
			return true
		}

	} else {
		if maxFileCount > 1 {
			//fmt.Println("file ",f.dir+"/"+f.filename ,"  fileSize ",fileSize(f.dir+"/"+f.filename) )
			if fileSize(f.dir+"/"+f.filename) >= maxFileSize {
				return true
			}
		}
	}
	return false
}

func (f *_FILE) resetConnect() {

	callback(f.dir+"/"+f.filename, Config)
}

func ResetLevel(level, model string) {
	if level == "" {
		level = "info"
	}
	logging.SetLevel(GetLoggingLevel(level), model)
}
func GetLoggingLevel(level string) logging.Level {

	switch level {
	case "info":
		return logging.INFO
	case "warning":
		return logging.WARNING
	case "error":
		return logging.ERROR
	case "debug":
		return logging.DEBUG
	}
	return logging.INFO
}

func (f *_FILE) rename() {
	if dailyRolling {
		fn := f.dir + "/" + f.filename + "." + f._date.Format(RENAMEDATEFORMAT)
		if !isExist(fn) && f.isMustRename() {
			if f.logfile != nil {
				f.logfile.Close()
			}
			err := os.Rename(f.dir+"/"+f.filename, fn)
			if err != nil {
				f.lg.Println("rename err", err.Error())
			}
			t, _ := time.Parse(DATEFORMAT, time.Now().Format(DATEFORMAT))
			f._date = &t
			f.logfile, _ = os.Create(f.dir + "/" + f.filename)
			f.lg = log.New(logObj.logfile, "\n", log.Ldate|log.Ltime|log.Lshortfile)
			f.resetConnect()
		}
	} else {
		f.coverNextOne()
	}
}

func (f *_FILE) nextSuffix() int {
	return int(f._suffix%int(maxFileCount) + 1)
}

func (f *_FILE) coverNextOne() {
	f._suffix = f.nextSuffix()
	if f.logfile != nil {
		f.logfile.Close()
	}
	if isExist(f.dir + "/" + f.filename + "." + strconv.Itoa(int(f._suffix))) {
		os.Remove(f.dir + "/" + f.filename + "." + strconv.Itoa(int(f._suffix)))
	}
	os.Rename(f.dir+"/"+f.filename, f.dir+"/"+f.filename+"."+strconv.Itoa(int(f._suffix)))

	f.logfile, _ = os.Create(f.dir + "/" + f.filename)
	f.lg = log.New(logObj.logfile, "\n", log.Ldate|log.Ltime|log.Lshortfile)
	f.resetConnect()
}

func fileSize(file string) int64 {
	f, e := os.Stat(file)
	if e != nil {
		fmt.Printf("stat err:%s\n", e.Error())
		return 0
	}
	return f.Size()
}

func isExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func fileMonitor() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			fileCheck()
		}
	}
}

func fileCheck() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if logObj != nil && logObj.isMustRename() {
		logObj.mu.Lock()
		defer logObj.mu.Unlock()
		logObj.rename()
	}
}

/*// LoggingInit is a 'hook' called at the beginning of command processing to
// parse logging-related options specified either on the command-line or in
// config files.  Command-line options take precedence over config file
// options, and can also be passed as suitably-named environment variables. To
// change module logging levels at runtime call `logging.SetLevel(level,
// module)`.  To debug this routine include logging=debug as the first
// term of the logging specification.
func LoggingInit(command string) {
	// Parse the logging specification in the form
	//     [<module>[,<module>...]=]<level>[:[<module>[,<module>...]=]<level>...]
	defaultLevel := loggingDefaultLevel
	var err error
	spec := viper.GetString("logging_level")
	if spec == "" {
		spec = viper.GetString("logging." + command)
	}
	if spec != "" {
		fields := strings.Split(spec, ":")
		for _, field := range fields {
			split := strings.Split(field, "=")
			switch len(split) {
			case 1:
				// Default level
				defaultLevel, err = logging.LogLevel(field)
				if err != nil {
					loggingLogger.Warningf("Logging level '%s' not recognized, defaulting to %s : %s", field, loggingDefaultLevel, err)
					defaultLevel = loggingDefaultLevel // NB - 'defaultLevel' was overwritten
				}
			case 2:
				// <module>[,<module>...]=<level>
				if level, err := logging.LogLevel(split[1]); err != nil {
					loggingLogger.Warningf("Invalid logging level in '%s' ignored", field)
				} else if split[0] == "" {
					loggingLogger.Warningf("Invalid logging override specification '%s' ignored - no module specified", field)
				} else {
					modules := strings.Split(split[0], ",")
					for _, module := range modules {
						logging.SetLevel(level, module)
						loggingLogger.Debugf("Setting logging level for module '%s' to %s", module, level)
					}
				}
			default:
				loggingLogger.Warningf("Invalid logging override '%s' ignored; Missing ':' ?", field)
			}
		}
	}

	// Set the default logging level for all modules
	logging.SetLevel(defaultLevel, "")
	loggingLogger.Debugf("Setting default logging level to %s for command '%s'", defaultLevel, command)
}*/

// DefaultLoggingLevel returns the fallback value for loggers to use if parsing fails
func DefaultLoggingLevel() logging.Level {
	return loggingDefaultLevel
}

// Initiate 'leveled' logging to stderr.
var Switcha bool

func Init() {
	Switcha = true
	for {
		t := time.Now()
		tim := t.String()[0:10] + "_" + t.String()[11:13]
		time.Sleep(1 * time.Second)
		t1 := time.Now()
		tim1 := t1.String()[0:10] + "_" + t.String()[11:13]

		if tim != tim1 || Switcha {
			//logFile := "eventserver.log." + tim[0:10] + "_" + tim[11:13]
			logFile := "eventserver.log." + tim1
			if Switcha {
				logFile = "eventserver.log"
			}
			Switcha = false

			format := logging.MustStringFormatter(
				"%{color}%{time:20060102150405.000} [%{module}] %{shortfunc} -> %{level:.4s} %{id:03x}%{color:reset} %{message}",
			)
			logFileDir := "loggings"
			Mkdirlog(logFileDir)
			//logFile := "eventserver.log"

			logs := logFileDir + "/" + logFile

			logIo, err := os.OpenFile(logs, os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				loggingLogger.Errorf("open log file err %s", err)
			}

			//SetRollingDaily(logFileDir, logFile,&viper.Viper,callback)
			//SetRollingFile(logFileDir,logFile,10000000,5,KB)

			backend := logging.NewLogBackend(logIo, "", 0) //err io not close

			//backend := logging.NewLogBackend(os.Stderr, "", 0)
			backendFormatter := logging.NewBackendFormatter(backend, format)

			logging.SetBackend(backendFormatter).SetLevel(loggingDefaultLevel, "")
			var serviceLog = logging.MustGetLogger("service")
			serviceLog.Info("log Division Init By Time")
		}
	}
}

package main

// server.go

import (
	"crypto/md5"
	"fmt"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"

	"io/ioutil"

	"encoding/hex"
	"eventservice/configMgr"
	"eventservice/db"
	sv "eventservice/example/serverproto"
	logFactory "eventservice/logFactory"
	"io"
	"runtime"
	"sync"
	"time"
)

var serviceLog = logging.MustGetLogger("service")

type Password string

func (p Password) Redacted() interface{} {
	return logging.Redact(string(p))
}

const (
	code1000 = "1000"
	code1001 = "1001"
	code1002 = "1002"
	code1003 = "1003"
	code1004 = "1004"
	code1005 = "1005"
	code1006 = "1006"
	code1007 = "1007"

	msg1000 = "交易成功"
	msg1001 = "交易进行中"
	msg1002 = "交易失败" //AppProcess Fail uchains返回码和这里对应
	msg1003 = "交易不存在"
	msg1004 = "交易异常"
	msg1005 = "未注册，请求失败"
	msg1006 = "共识前检查异常" //BeforeConsCheck Fail
	msg1007 = "共识后检查异常" //AfterConsCheckAndUpdateData Fail

	msgRegist01 = "该ip-port已注册"
	msgRegist02 = "注册成功"
	msgRegist03 = "注册失败"
	msgRegist04 = "交易异常"

	constAmount     = 1 / 3 // 1/3容错
	constSendAmount = 100   // 发送30次失败则不再发送
)

//server核心
type server struct {
	ec                  *configMgr.EventConfig
	dh                  *db.DbHandler
	addressIdMap        map[string]string
	updateIspushedChan  chan *UpdateIspushedsql
	switchButton        bool
	totalEventTxid      int32
	totalEventCountTxid int32
}

// 计数收集投票
var GoChainRequestCountAscChan chan *GoChainRequestCountAsc

// 异步处理投票
type GoChainRequestCountAsc struct {
	req *sv.ChainTranscationAccountReq
}

// 更新推送状态
type UpdateIspushedsql struct {
	sql string
}

// 处理txid
var GoChainRequestReqAscChan chan *GoChainRequestReqAsc

// 异步处理txid
type GoChainRequestReqAsc struct {
	req *sv.ChainTranscationReq
}

// 缓存交易情况的map
var TxidsMap *sync.Map

// 缓存send java消息的stream 判断是不是重启java客户端 造成stream重复
var StreamMap *sync.Map

// 缓存交易情况的map 中的value
type VoteAccount struct {
	txid       string
	totalNodes int32
	//srsu            sync.RWMutex
	//srfa            sync.RWMutex
	votesSuccessMap map[string]string
	votesFailedMap  map[string]string
	txtask          *time.Timer
	chainId         string
	address         string
	isUpdate        bool
}

// 缓存ip地址对应的
var AddressMap *sync.Map

var AddressCount int

//
type ClientTransactionJavaReq struct {
	TxId       string
	Ecode      string
	Emessage   string
	ChainId    string
	Address    string
	SendAmount int32
}

type ClientQuickReq struct {
	QuickSwitch  bool
	Address      string
	AddressCount int
}

//sdk请求txid 获得交易结果
var ClientTransactionJavaReqChan chan *ClientTransactionJavaReq
var ClientQuickReqChan chan *ClientQuickReq

// 建表字段 ghc date 2018年9月25日10点41分
type ASSETFIELDNAME string

const (
	ID          ASSETFIELDNAME = "ID"
	IP          ASSETFIELDNAME = "IP"
	ECODE       ASSETFIELDNAME = "ECODE"
	EMESSAGE    ASSETFIELDNAME = "EMESSAGE"
	CHAINID     ASSETFIELDNAME = "CHAINID"
	TXID        ASSETFIELDNAME = "TXID"
	ECLIENTPORT ASSETFIELDNAME = "ECLIENTPORT"
	ECLIENTIP   ASSETFIELDNAME = "ECLIENTIP"
	ETIME       ASSETFIELDNAME = "ETIME"
	REMARK      ASSETFIELDNAME = "REMARK"
	PORT        ASSETFIELDNAME = "PORT"
	TXIP        ASSETFIELDNAME = "TXIP"
	TOTALNODES  ASSETFIELDNAME = "TOTALNODES"
	ISPUSHED    ASSETFIELDNAME = "ISPUSHED"
)

type MsgHandler interface {
	SendToJavaMsg(javaMsg *sv.ClientTransactionJavaReq) error
}

/**
* @Title: service.go
* @Description: GoClientRegistEvent  注册 该方法调用次数少 无须分离逻辑
* @author ghc
* @date 9/25/18 16:50 PM
* @version V1.0
 */
func (s *server) GoClientRegistEvent(ctx context.Context, request *sv.ClientRegisterAddressReq) (*sv.ClientRegisterAddressRes, error) {
	ip := request.AddRessIpReq
	//port := request.AddRessPortReq
	remarkReq := request.RemarkReq
	tm := time.Now().UnixNano()
	ipPort := ip
	sql := fmt.Sprintf("select count(*) as acount from %s where %s ='%s'",
		s.ec.Config.RegisterTableName, ECLIENTIP, ip)
	serviceLog.Info("findRepeat sql", sql)

	rows, err := s.dh.Db.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("findRepeat err", err)
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist04, IsSuccess: false, MessageIDRes: ""}, err
	}
	defer rows.Close()
	var acount int
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&acount)
			if err != nil {
				return &sv.ClientRegisterAddressRes{MessageRes: msgRegist04, IsSuccess: false, MessageIDRes: ""}, err
			}
		}
	} else {
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist03, IsSuccess: false, MessageIDRes: ""}, nil
	}

	//去重
	//给注册信息分配hash id
	Md5Inst := md5.New()
	Md5Inst.Write([]byte(ipPort))
	id := Md5Inst.Sum([]byte(""))
	idStr := hex.EncodeToString(id)
	// 缓存ip 对应地址 推送消息时使用
	AddressMap.Store(idStr, ip)

	if acount == 0 {

		//拼接sql
		sqlValue := fmt.Sprintf("('%s','%s','%d','%s')",
			idStr,
			ip,
			tm,
			remarkReq,
		)
		sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s) "+
			"values",
			s.ec.Config.RegisterTableName,
			ID,
			ECLIENTIP,
			ETIME,
			REMARK,
		)
		sqlFinal := sqlSentence + sqlValue

		//写库
		serviceLog.Info("sqlFinal is ", sqlFinal)
		_, err = s.dh.Db.Exec(sqlFinal)
		if err != nil {
			/*ph.DataCacheMap.Delete(sc.DataHash)*/
			serviceLog.Errorf("write db err:%s", err.Error())
		}
		s.addressIdMap[idStr] = ipPort //放在缓存中
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist02, IsSuccess: true, MessageIDRes: idStr}, nil
	} else {
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist01, IsSuccess: false, MessageIDRes: idStr}, nil
	}

}

/**
* @Title: service.go
* @Description: GoClientRequestEvent  处理客户端请求txid 该方法调用次数少 无须分离逻辑
* @author ghc
* @date 9/25/18 16:50 PM
* @version V1.0
 */
func (s *server) GoClientRequestEvent(ctx context.Context, request *sv.ClientTransactionReq) (*sv.ClientTransactionRes, error) {
	addressId := request.AddressIdReq
	chainId := request.ChainIdReq
	txid := request.TxIdReq

	cap, ok := s.addressIdMap[addressId] //先判断是否注册
	serviceLog.Info("GoClientRequestEvent s.addressIdMap", s.addressIdMap)
	if !ok {
		serviceLog.Info("addressId Non-existent cap:", cap, "addressId Non-existent", addressId)
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1005, MessageRes: msg1005, TimeRes: "", ChainIdRes: ""}, nil
	}

	sql := fmt.Sprintf("select %s,%s,%s,%s,%s from %s where %s = '%s' and %s ='%s'",
		TXID, ECODE, EMESSAGE, ETIME, CHAINID, s.ec.Config.EventmsgtableName, TXID, txid, CHAINID, chainId)
	serviceLog.Info("RequestEvent sql", sql)

	rows, err := s.dh.Db.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("GoClientRequestEvent err", err)
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1004, MessageRes: msg1004, TimeRes: "", ChainIdRes: ""}, err
	}
	defer rows.Close()
	if rows != nil {
		for rows.Next() {

			var txidr string
			var ecoder string
			var emessager string
			var etimer string
			var chainIdr string
			serviceLog.Info("txidr:", txidr, "---ecoder:", ecoder, "---emessager:", emessager, "---etimer:", etimer, "---chainIdr:", chainIdr)
			err = rows.Scan(&txidr, &ecoder, &emessager, &etimer, &chainIdr)
			if err != nil {
				fmt.Println("GoClientRequestEvent err", err)
				serviceLog.Error("GoClientRequestEvent err", err)
				return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1004, MessageRes: msg1004, TimeRes: "", ChainIdRes: ""}, err
			}
			return &sv.ClientTransactionRes{TxIdRes: txidr, CodeRes: ecoder, MessageRes: emessager, TimeRes: etimer, ChainIdRes: chainIdr}, err
		}

	} else {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1003, MessageRes: msg1003, TimeRes: "", ChainIdRes: ""}, err
	}

	return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1003, MessageRes: msg1003, TimeRes: "", ChainIdRes: ""}, err
}

/**
* @Title: service.go
* @Description: GoChainRequestEvent  uchains commitx阶段 收集txid
* @author ghc
* @date 9/27/18 15:31 PM
* @version V1.0
 */
func (s *server) GoChainRequestEvent(stream sv.GoEventService_GoChainRequestEventServer) error {
	/*	// 启动十个协成 处理接收的交易id
		for i := 0; i < 10; i++ {
			fmt.Println("enter GoChainRequestEvent")
			go s.GoChainRequestAscEvent()
		}*/
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			fmt.Println("Server  GoChainRequestEvent Stream ERR", err)
			serviceLog.Error("Server GoChainRequestEvent Stream recv err", err)
			stream.Send(&sv.ChainTranscationRes{TxIdRes: "", IsReceivedRes: false})
			return err
		}
		if req != nil {
			_, ok := TxidsMap.Load(req.TxIdReq)                                        //先缓存查询 若不存在，则取查询数据库
			serviceLog.Info("--------A-----------------------", s.totalEventTxid)      //todo 后面去掉这个日志  todo 以后去掉 测试tps观察使用
			serviceLog.Info("--------B-----------------------", s.totalEventCountTxid) //todo 后面去掉这个日志  todo 以后去掉 测试tps观察使用
			if !ok {
				gasc := &GoChainRequestReqAsc{}
				gasc.req = req
				GoChainRequestReqAscChan <- gasc
				s.totalEventTxid++
			}
			err = stream.Send(&sv.ChainTranscationRes{TxIdRes: req.TxIdReq, IsReceivedRes: true})
			if err != nil {
				serviceLog.Error(req.TxIdReq + "Server Stream send fail")
				return err
			}
		} else {
			serviceLog.Warning("GoChainRequestEvent stream.Recv() req is nil")
		}
	}

}

/**
* @Title: service.go
* @Description: GoChainRequestAscEvent  异步处理 收集txid
* @author ghc
* @date 9/27/18 15:31 PM
* @version V1.0
 */
func (s *server) GoChainRequestAscEvent() error {
	for {
		select {
		case asc := <-GoChainRequestReqAscChan:

			serviceLog.Info("--------AAA-----------------------", s.totalEventTxid) // todo 以后去掉 测试tps观察使用
			req := asc.req                                                          //req 上一步已经做了nil判断
			reqTxId := req.TxIdReq
			reqTxIp := req.TxIpReq
			reqTotalNotes := req.TotalVotesReq
			reqChainId := req.ChainIdReq
			reqEcode := code1001
			reqMessage := msg1001
			isPushed := 0 //默认未推送
			etime := time.Now().UnixNano()
			//_, ok := TxidsMap.Load(reqTxId) //先缓存查询 若不存在，则取查询数据库
			//if !ok {

			sql := fmt.Sprintf("select count(*) as acount from %s where %s = '%s'",
				s.ec.Config.EventmsgtableName, TXID, reqTxId)
			serviceLog.Info("findRepeat sql", sql)

			rows, err := s.dh.Db.Query(sql) //查询去重
			if err != nil {
				serviceLog.Error("findRepeat err", err)
				return err
			}
			var acount int
			if rows != nil {
				for rows.Next() {
					err = rows.Scan(&acount)

					if err != nil {
						return err
					}

				}
				rows.Close()
			} else {
				return nil
			}

			if acount == 0 {
				//拼接sql
				sqlValue := fmt.Sprintf("('%s','%s','%s','%d','%s','%s','%d','%d')",
					reqTxId,
					reqEcode,
					reqMessage,
					etime,
					reqChainId,
					reqTxIp,
					reqTotalNotes,
					isPushed,
				)
				sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s,%s,%s,%s,%s) "+
					"values",
					s.ec.Config.EventmsgtableName,
					TXID,
					ECODE,
					EMESSAGE,
					ETIME,
					CHAINID,
					TXIP,
					TOTALNODES,
					ISPUSHED,
				)
				sqlFinal := sqlSentence + sqlValue

				//写库
				serviceLog.Info("sqlFinal is ", sqlFinal)
				_, err = s.dh.Db.Exec(sqlFinal)
				if err != nil {
					/*ph.DataCacheMap.Delete(sc.DataHash)*/
					serviceLog.Errorf("write db err:%s", err.Error())
				}
			}
			voteMap := VoteAccount{}
			voteMap.txid = reqTxId
			voteMap.chainId = reqChainId
			voteMap.address = reqTxIp
			//voteMap.address = "10.10.70.146" //todo 暂时改掉ip  ****
			voteMap.totalNodes = reqTotalNotes
			voteMap.votesSuccessMap = make(map[string]string)
			voteMap.votesFailedMap = make(map[string]string)
			voteMap.txtask = time.AfterFunc(120*time.Second, func() {
				TaskEvent(reqTxId, s)
			})
			TxidsMap.Store(reqTxId, voteMap) //缓存txid和票数
			/*valu, bool := TxidsMap.Load(reqTxId)
			if bool {
				fmt.Println("TxidsMap value", valu)
			}*/

			//serviceLog.Info("TxidsMap", TxidsMap)
		}
		//}
	}

}

/**
* @Title: service.go
* @Description: GoChainRequestCountEvent  uchains 交易统计阶段 收集votes
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *server) GoChainRequestCountEvent(stream sv.GoEventService_GoChainRequestCountEventServer) error {
	// 十个线程处理任务
	/*for i := 0; i < 10; i++ {
		fmt.Println("enter GoChainRequestCountEvent")
		go s.GoChainRequestCountAscEvent()
	}*/
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			serviceLog.Error("Server GoChainRequestCountEvent Stream recv err", err)
			stream.Send(&sv.ChainTranscationAccountRes{TxIdRes: "", IsReceivedRes: false})
			return err
		}
		asc := &GoChainRequestCountAsc{}
		if req != nil {

			asc.req = req
			GoChainRequestCountAscChan <- asc

			err = stream.Send(&sv.ChainTranscationAccountRes{req.TxIdReq, true})
		}
	}
}

/**
* @Title: service.go
* @Description: GoChainRequestCountAscEvent  uchains 交易统计阶段 收集votes 单独处理
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *server) GoChainRequestCountAscEvent() error {
	for {
		select {
		case asc := <-GoChainRequestCountAscChan:
			req := asc.req // req is nil 上一步已经判断
			txidreq := req.TxIdReq
			codereq := req.CodeReq
			messreq := req.MessageReq
			issuccreq := req.IsSuccessReq
			nodeidreq := req.NodeIdReq
			value, ok := TxidsMap.Load(txidreq) //map 中不存在，
			if !ok {
				serviceLog.Warning(txidreq, "hash handle over or txid not exit")
			} else {

				voteVal := value.(VoteAccount)

				totalNods := int32(voteVal.totalNodes)*1/3 + 1
				//todo 统计票数日志 适时删除
				serviceLog.Info("int32(len(voteVal.votesSuccessMap))", float32(len(voteVal.votesSuccessMap)))
				serviceLog.Info("voteVal.totalNodes*1/3", totalNods)
				serviceLog.Info("voteVal.totalNodes", voteVal.totalNodes)
				var code string
				var msg string
				if issuccreq {
					//写锁
					//voteVal.srsu.Lock()
					voteVal.votesSuccessMap[nodeidreq] = txidreq //nodeId 作为key 避免票数重复
					//voteVal.srsu.Unlock()

					TxidsMap.Store(txidreq, voteVal)

					/*TMapRwlock.RLock()
					value1, ok1 := TxidsMap.Load(txidreq) //map 中不存在，
					TMapRwlock.RUnlock()

					fmt.Println("value1:", value1, "ok1:", ok1)*/
					voteAmount := int32(len(voteVal.votesSuccessMap))
					succ := voteAmount >= totalNods
					if succ {
						// 只要满足记账要求就发送
						if !voteVal.isUpdate {
							// 避免两票/三票重复发
							voteVal.isUpdate = true
							TxidsMap.Store(txidreq, voteVal)
							code = code1000
							msg = msg1000
							sqlFinal := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
								s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txidreq)
							serviceLog.Info("GoChainRequestCountEvent sqlFinal", sqlFinal)
							_, err := s.dh.Db.Exec(sqlFinal)
							if err != nil {
								serviceLog.Error("GoChainRequestCountEvent db set ecode fail txid", txidreq, "GoChainRequestCountEvent sqlFinal err", err)
							} else {
								s.totalEventCountTxid++
								//TxidsMap.Delete(txidreq)
								tarnsJavaReq := &ClientTransactionJavaReq{}
								tarnsJavaReq.TxId = txidreq
								tarnsJavaReq.ChainId = voteVal.chainId
								tarnsJavaReq.Ecode = code
								tarnsJavaReq.Emessage = msg
								tarnsJavaReq.SendAmount = 0
								ClientTransactionJavaReqChan <- tarnsJavaReq
								serviceLog.Info("GoJavaRequestCountEvent hash send txid:", txidreq, msg)
								if err != nil {
									serviceLog.Error(txidreq + "Server Stream send fail")
									return err
								}
							}
						}

					}
				} else {
					//写锁
					//voteVal.srfa.Lock()
					voteVal.votesFailedMap[nodeidreq] = txidreq
					//voteVal.srfa.Unlock()
					TxidsMap.Store(txidreq, voteVal)
					voteAmount := int32(len(voteVal.votesFailedMap))
					fail := voteAmount >= totalNods
					if fail {
						code = codereq //todo 这里的失败原因使用的uchains返回的
						msg = messreq  //
						sqlFinal := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
							s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txidreq)
						serviceLog.Info("GoChainRequestCountEvent sqlFinal", sqlFinal)
						_, err := s.dh.Db.Exec(sqlFinal)
						if err != nil {
							serviceLog.Error("GoChainRequestCountEvent db set ecode fail txid", txidreq)
						} else {
							s.totalEventCountTxid++
							//TxidsMap.Delete(txidreq)
							tarnsJavaReq := &ClientTransactionJavaReq{}
							tarnsJavaReq.TxId = txidreq
							tarnsJavaReq.ChainId = voteVal.chainId
							tarnsJavaReq.Ecode = code
							tarnsJavaReq.Emessage = msg
							ClientTransactionJavaReqChan <- tarnsJavaReq
						}

					}
				}

			}
		}
	}

}

/**
* @Title: service.go
* @Description: GoJavaRequestEvent  uchains 交易成功 推送消息到java服务器
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *server) GoJavaRequestEvent(stream sv.GoEventService_GoJavaRequestEventServer) error {
	serviceLog.Info("Server GoJavaRequestEvent enter")
	var address string
	s.switchButton = false
	req, err := stream.Recv()
	if err == io.EOF {
		fmt.Println("read done")
		return nil
	}
	if err != nil {
		fmt.Println("Server GoJavaRequestEvent Stream ERR", err)
		serviceLog.Error("Server Stream recv err", err)
		return err
	}
	if req != nil {
		fmt.Println("GoJavaRequestEvent req", req.TxIdRes)
		address = req.TxIdRes
	}
	address = req.TxIdRes
	value, ok := StreamMap.Load(address)
	if ok {
		serviceLog.Info("shut down channel  address", value)
		cq := &ClientQuickReq{}
		cq.QuickSwitch = true
		cq.Address = address
		AddressCount = AddressCount + 1
		cq.AddressCount = AddressCount
		ClientQuickReqChan <- cq
	}
	StreamMap.Store(address, address)

	for {
		if !s.switchButton {
			go func() {
				//time.Sleep(20)
				s.SendToJavaMsg(stream, address, AddressCount)
			}()
		}
		req, err = stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			fmt.Println("Server GoJavaRequestEvent Stream ERR", err)
			serviceLog.Error("Server Stream recv err", err)
			return err
		}

	}

}

/**
* @Title: service.go
* @Description: SendToJavaMsg  uchains 交易成功 推送消息到java服务器
* @author ghc
* @date 9/29/18 10:47 AM
* @version V1.0
 */
func (s *server) SendToJavaMsg(stream sv.GoEventService_GoJavaRequestEventServer, address string, addressCount int) {
	var ipr string
	s.switchButton = true
	var voteValAddress string
	// 注册时的IP 地址 对应返回的address
	ip, ok := AddressMap.Load(address)
	//fmt.Println("SendToJavaMsg ip", ip)
	//缓存不存在 去数据库中查
	if ip == "" || !ok {
		sql := fmt.Sprintf("select %s  from %s where %s = '%s'",
			ECLIENTIP, s.ec.Config.RegisterTableName, ID, address)
		serviceLog.Info("findRepeat sql", sql)

		rows, err := s.dh.Db.Query(sql) //查询去重
		if err != nil {
			serviceLog.Error("SendToJavaMsg findIp err", err)
		}
		defer rows.Close()
		if rows != nil {
			for rows.Next() {
				err = rows.Scan(&ip)
				if err != nil {
					serviceLog.Error("SendToJavaMsg findIp rows err", err)
				}
			}
		}
	}

	/*	fmt.Println("SendToJavaMsg has send")
		for i := 0; i < 10; i++ {
			err := stream.Send(&sv.ClientTransactionJavaReq{"11356456", "1001", "发送成功", "coupon"})
			if err != nil {
				serviceLog.Error("11356456"+":Server Stream send fail erro", err)
				return err
			}
		}
		return nil*/

	for {
		select {
		//tx 成功或失败  推送消息
		case cj := <-ClientTransactionJavaReqChan:
			//fmt.Println("GoJavaRequestEvent hash receive txid:", cj.TxId, cj.Emessage) //todo 如果不注释掉 发送不成功的会一直刷日志
			serviceLog.Info("GoJavaRequestEvent hash receive txid:", cj.TxId, cj.Emessage)
			txidd := cj.TxId
			// 从交易缓存中获取txid 对应的 IP

			val, ok := TxidsMap.Load(txidd)

			if !ok {
				sql := fmt.Sprintf("select %s  from %s where %s = '%s'",
					TXIP, s.ec.Config.EventmsgtableName, TXID, txidd)
				serviceLog.Info("find s.ec.Config.EventmsgtableName findIp sql", sql)

				rows, err := s.dh.Db.Query(sql) //查询去重
				if err != nil {
					serviceLog.Error("find s.ec.Config.EventmsgtableName findIp err", err)
				}
				defer rows.Close()
				if rows != nil {
					for rows.Next() {
						err = rows.Scan(&ipr)
						if err != nil {
							serviceLog.Error("SendToJavaMsg findIp rows err", err)
						} // 没必要查出来继续放进缓存中 因为接下来处理完会马上删掉
					}
				}

			} else {
				voteVal := val.(VoteAccount)
				voteValAddress = voteVal.address
				serviceLog.Info("txid", cj.TxId, "voteValAddress", voteValAddress, "ip", ip, "ipr", ipr)
			}

			if voteValAddress == ip || ipr == ip {
				err := stream.Send(&sv.ClientTransactionJavaReq{cj.TxId, cj.Ecode, cj.Emessage, cj.ChainId})
				if err != nil {
					serviceLog.Error(cj.TxId+":Server Stream send fail erro", err)
					// 出错代表没发送成功 重试次数30次 继续塞入管道
					if cj.SendAmount <= constSendAmount {
						time.Sleep(1 * time.Second)
						cj.SendAmount++
						ClientTransactionJavaReqChan <- cj
					} else {
						TxidsMap.Delete(txidd) // 30次后再从缓存中删除 需要主动去查询了
					}
					//return err
				} else {
					TxidsMap.Delete(txidd) // 发送成功再从缓存中删除
					serviceLog.Info("GoJavaRequestEvent send txid success:", cj.TxId)
					sqlFinal := fmt.Sprintf("update %s set %s = '%d'  where %s = '%s'",
						s.ec.Config.EventmsgtableName, ISPUSHED, 1, TXID, cj.TxId)
					serviceLog.Info("update ispushed sqlFinal", sqlFinal)
					usql := &UpdateIspushedsql{}
					usql.sql = sqlFinal
					//异步处理sqlupdate ispushed
					s.updateIspushedChan <- usql

				}
			} else {
				// 若消息取出来判断无法发送 则重新塞入管道中
				time.Sleep(1 * time.Second)
				ClientTransactionJavaReqChan <- cj
			}
		case qk := <-ClientQuickReqChan:
			if qk.QuickSwitch && qk.Address == address && qk.AddressCount > addressCount {
				// 退出
				return
			}

		}
	}

}

/**
* @Title: service.go
* @Description: TaskEvent   定时器处理阶段
* @author ghc
* @date 9/27/18 16:55 PM
* @version V1.0
 */
func TaskEvent(txid string, s *server) {
	value, ok := TxidsMap.Load(txid) //map 中不存在，
	if ok {
		serviceLog.Info("enter TaskEvent txid", txid)
		voteVal := value.(VoteAccount)
		totalNods := voteVal.totalNodes*1/3 + 1
		voteAmountSu := int32(len(voteVal.votesSuccessMap))
		voteAmountFal := int32(len(voteVal.votesFailedMap))
		succ := voteAmountSu >= totalNods
		fail := voteAmountFal >= totalNods
		serviceLog.Info("txid", txid, "totalNods:", totalNods, "voteAmountSu:", voteAmountSu)
		var code string
		var msg string
		if succ || fail {
			if succ {
				code = code1000
				msg = msg1000
			}
			if fail {
				code = code1002
				msg = msg1002
			}
			sql := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
				s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txid)
			serviceLog.Info("update sql", sql)

			_, err := s.dh.Db.Exec(sql) //更新状态
			if err == nil {
				serviceLog.Info("txid write db success", txid)
				tarnsJavaReq := &ClientTransactionJavaReq{}
				tarnsJavaReq.TxId = txid
				tarnsJavaReq.ChainId = voteVal.chainId
				tarnsJavaReq.Ecode = code
				tarnsJavaReq.Emessage = msg
				ClientTransactionJavaReqChan <- tarnsJavaReq //传进通道 调用 response服务端方法
				//TxidsMap.Delete(txid)                        //成功 删除缓存 放在了发送给java成功再删除
			} else {
				serviceLog.Error("txid write db failed", txid)
			}
		}
	} else {
		//每个节点投票只有一次机会 若缓存中不存在，则代表记录票数丢失，只能提示警告
		serviceLog.Warning("TaskEvent txid TxidsMap.Load(txid) fail", txid)
	}

}

/*func (s *server) GoChainRequestCountEvent(ctx context.Context) (*sv.GoEventService_GoChainRequestEventClient, error) {
	return nil, nil
}*/

/**
* @Title: service.go
* @Description:  server struct init
* @author ghc
* @date 9/25/18 16:05 PM
* @version V1.0
 */
func (s *server) init() {
	serviceLog.Debug("enter init function success")
	dhm := &db.DbHandler{}
	dbh, err := dhm.GetDbHandler()
	if err != nil {
		serviceLog.Error("getDbHandler fail", err)
		return
	}
	s.dh = dbh
	ecof := &configMgr.EventConfig{}
	evcf, err := ecof.NewEventConfig()
	if err != nil {
		serviceLog.Error("newEventConfig fail", err)
	}
	s.ec = evcf
	s.addressIdMap = make(map[string]string)
	TxidsMap = &sync.Map{}
	StreamMap = &sync.Map{}                                                      //初始化缓存Ip地址map
	AddressMap = &sync.Map{}                                                     //缓存消息票数的队列
	ClientTransactionJavaReqChan = make(chan *ClientTransactionJavaReq, 1000000) //缓冲100万条数据
	GoChainRequestReqAscChan = make(chan *GoChainRequestReqAsc, 1000000)
	GoChainRequestCountAscChan = make(chan *GoChainRequestCountAsc, 1000000)
	s.updateIspushedChan = make(chan *UpdateIspushedsql, 1000000)
	ClientQuickReqChan = make(chan *ClientQuickReq, 100) // send java 消息退出管道
	// 启动十个协成 处理接收的交易id
	go func() {
		fmt.Println("enter GoChainRequestEvent")
		s.GoChainRequestAscEvent()
	}()
	go func() {
		fmt.Println("enter GoChainRequestCountEvent")
		s.GoChainRequestCountAscEvent()
	}()
	/*	for i := 0; i < 10; i++ {
		fmt.Println("enter GoChainRequestCountEvent")
		go s.GoChainRequestCountAscEvent()
	}*/
}

func (s *server) updateIspushed() {
	for {
		select {
		case upush := <-s.updateIspushedChan:
			_, err := s.dh.Db.Exec(upush.sql) //更新状态
			if err != nil {
				serviceLog.Info("upush.sql update fail", upush.sql)
			} else {
				// 若失败了 则放回队列
				time.Sleep(1 * time.Second)
				s.updateIspushedChan <- upush
			}
		}
	}
}

/**
* @Title: service.go
* @Description:  server struct createTable
* @author ghc
* @date 9/25/18 16:05 PM
* @version V1.0
 */
func (s *server) createTable() {
	//初始化server struct
	s.init()

	fmt.Println("init success")
	dh := s.dh
	//建表 events_client_address
	// todo linux 下目录使用 ../docs/database/registerDb.sql
	sys := string(runtime.GOOS) // 判断操作系统
	var sqlBytes []byte
	var err error
	if sys == "windows" {
		sqlBytes, err = ioutil.ReadFile("docs/database/registerDb.sql")
	} else {
		sqlBytes, err = ioutil.ReadFile("../docs/database/registerDb.sql")
	}
	if err != nil {
		serviceLog.Error("ioutil.ReadFile sqlBytes err", err)
		return
	}
	sqlTable := string(sqlBytes)
	fmt.Println("sqlTable", sqlTable)
	result, err := dh.Db.Exec(sqlTable)
	if err != nil {
		serviceLog.Error("createTable err", err, result)
		return
	}
	//建表 events_msg
	// todo linux 下目录使用 ../docs/database/eventDb.sql
	var sqlBytes2 []byte
	if sys == "windows" {
		sqlBytes2, err = ioutil.ReadFile("docs/database/eventDb.sql")
	} else {
		sqlBytes2, err = ioutil.ReadFile("../docs/database/eventDb.sql")
	}
	if err != nil {
		serviceLog.Error("ioutil.ReadFile sqlBytes2 err", err)
		return
	}
	sqlTable2 := string(sqlBytes2)
	fmt.Println("sqlTable2", sqlTable2)
	result2, err := dh.Db.Exec(sqlTable2)
	if err != nil {
		serviceLog.Error("createTable err", err, result2)
		return
	}
	fmt.Println("createTable success")
	serviceLog.Info("createTable success")
	// 启动协成监听mysql push更新
	for i := 0; i < 10; i++ {
		go s.updateIspushed()
	}

}

//todo  抽象出公共部分  以后完善
/*func (s *server) commonCountEventHandle(succc bool, fail bool, txidreq string, voteVal VoteAccount, nodeidreq string, totalNods int32, stream sv.GoEventService_GoChainRequestCountEventServer) {
	voteVal.votesSuccessMap[txidreq] = nodeidreq
	TxidsMap.Store(txidreq, voteVal)
	voteAmount := int32(len(voteVal.votesSuccessMap))
	succ := voteAmount >= totalNods
	if succ {
		code = code1000
		msg = msg1000
		sqlFinal := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
			s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txidreq)
		_, err := s.dh.Db.Exec(sqlFinal)
		if err != nil {
			serviceLog.Error("GoChainRequestCountEvent db set ecode fail txid", txidreq)
		} else {
			tarnsJavaReq := &ClientTransactionJavaReq{}
			tarnsJavaReq.TxId = txidreq
			tarnsJavaReq.ChainId = voteVal.chainId
			tarnsJavaReq.Ecode = code
			tarnsJavaReq.Emessage = msg
			ClientTransactionJavaReqChan <- tarnsJavaReq
			err = stream.Send(&sv.ChainTranscationAccountRes{txidreq, true})
			if err != nil {
				serviceLog.Error(txidreq + "Server Stream send fail")
				return err
			}
		}
	}
}*/

func main() {
	// 初始化日志
	go func() {
		logFactory.Init()
	}()
	time.Sleep(5 * time.Second)
	serv := &server{}
	serv.createTable()
	lis, err := net.Listen("tcp", db.Port)
	if err != nil {
		fmt.Println("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	sv.RegisterGoEventServiceServer(s, serv)
	s.Serve(lis)

	//test 使用
	/*se := &server{}
	se.createTable()*/
}

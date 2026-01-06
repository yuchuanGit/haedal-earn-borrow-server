package logic

import (
	"haedal-earn-borrow-server/common"
	"haedal-earn-borrow-server/common/mydb"
	"haedal-earn-borrow-server/common/rpcSdk"
	"log"
	"strconv"
	"time"
)

const (
	MultiplyUpdateMarketEvent = "::multiply::MultiplyUpdateMarketEvent" //

)

func ScanMultiplyEvent(nextCursor string) {
	reqParams := rpcSdk.SuiTransactionBlockInputParameter(rpcSdk.MultiplycfgObjectId, nextCursor)
	resp := rpcSdk.EventRpcRequest(reqParams, "ScanMultiplyEvent")
	common.EventsCursorUpdate(resp.NextCursor, common.ScheduledTaskTypeMultiply)
	if len(resp.Data) == 0 {
		log.Printf("ScanMultiplyEvent lastCursor=%v\n", nextCursor)
		return
	}
	for _, data := range resp.Data {
		digest := data.Digest
		transactionTimeUnix := data.TimestampMs
		for _, event := range data.Events {
			eventType := EventType(event.Type)
			switch eventType {
			case MultiplyUpdateMarketEvent: // market激励
				MultiplyUpdateMarket(event.ParsedJson, digest, transactionTimeUnix)
			}
		}
	}
	ScanMultiplyEvent(resp.NextCursor)
}

func MultiplyUpdateMarket(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	valid := parsedJson["valid"]
	caller := parsedJson["caller"].(string)
	hearn_addr := parsedJson["hearn_addr"].(string)
	timestamp_ms_unix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestamp_ms_unix, 10, 64)
	if convErr != nil {
		log.Printf("MultiplyUpdateMarket 转换失败：%v\n", convErr)
	}
	timestamp_ms := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := mydb.GetDbConnection()
	isInsert := true
	queryRs, queryErr := con.Query("select * from multiply_update_market where market_id=?", market_id)
	if queryErr != nil {
		log.Printf("multiply_update_market查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		isInsert = false
	}
	sql := "insert into multiply_update_market(valid,caller,hearn_addr,digest,timestamp_ms_unix,timestamp_ms,transaction_time_unix,transaction_time,market_id) value(?,?,?,?,?,?,?,?,?)"
	if !isInsert {
		sql = "update multiply_update_market set valid=?,caller=?,hearn_addr=?,digest=?,timestamp_ms_unix=?,timestamp_ms=?,transaction_time_unix=?,transaction_time=? where market_id=?"

	}
	result, err := con.Exec(sql, valid, caller, hearn_addr, digest, timestamp_ms_unix, timestamp_ms, transactionTimeUnix, transactionTime, market_id)
	if err != nil {
		log.Printf("multiply_update_market新增或者更新失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.RowsAffected()
	log.Printf("multiply_update_market更新行：=%v", lastInsertID)
	defer con.Close()
}

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
	FarmingPoolCreateEvent         = "::events::FarmingPoolCreateEvent"            // 创建FarmingPool
	FarmingRewardConfigAddEvent    = "::events::FarmingRewardConfigAddEvent"       //
	FarmingRewardBankFundEvent     = "::events::FarmingRewardBankFundEvent"        //
	FarmingRewardBankExtractEvent  = "::events::FarmingRewardBankExtractEvent"     //
	FarmingRewardConfigUpdateEvent = "::events::FarmingRewardConfigUpdateEvent"    //
	FarmingPoolPauseEvent          = "::events::FarmingPoolPauseEvent"             //
	FarmingPoolResumeEvent         = "::events::FarmingPoolResumeEvent"            //
	FarmingStakeEvent              = "::events::FarmingStakeEvent"                 //
	FarmingUnstakeEvent            = "::events::FarmingUnstakeEvent"               //
	FarmingClaimEvent              = "::meta_vault_events::FarmingClaimEvent"      //
	FarmingRoleUpdateEvent         = "::meta_vault_events::FarmingRoleUpdateEvent" //
	FarmingMigrateEvent            = "::meta_vault_events::FarmingMigrateEvent"    //
)

func RpcRequestScanCreateFarming(nextCursor string) {
	log.Printf("RpcRequestScanCreateFarming nextCursor=%v\n", nextCursor)
	reqParams := rpcSdk.SuiTransactionBlockInputParameter(FarmingObjectId, nextCursor)
	resp := rpcSdk.EventRpcRequest(reqParams, "ScanCreateFarming")
	common.EventsCursorUpdate(resp.NextCursor, common.ScheduledTaskTypeFarming)
	if len(resp.Data) == 0 {
		log.Printf("RpcRequestScanCreateFarming lastCursor=%v\n", nextCursor)
		return
	}
	for _, data := range resp.Data {
		digest := data.Digest
		transactionTimeUnix := data.TimestampMs
		for _, event := range data.Events {
			eventType := EventType(event.Type)
			switch eventType {
			case FarmingPoolCreateEvent: // 创建FarmingPool
				InsertFarmingPoolCreate(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingRoleUpdateEvent: //角色权限更新
				InsertFarmingRoleUpdate(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingMigrateEvent: //合约版本迁移
				InsertFarmingMigrate(event.ParsedJson, digest, transactionTimeUnix)
			}
		}
	}
	RpcRequestScanCreateFarming(resp.NextCursor)
}

func ScanFarmingEvent() {
	jobTasks := common.QueryExecutionInputObjectId(common.ScheduledTaskTypeFarmingPoolId, "ScanFarmingEvent")
	isFinalTask := false
	lastIdx := len(jobTasks) - 1
	for idx, jobTask := range jobTasks {
		if idx == lastIdx {
			isFinalTask = true
		}
		RpcRequestScanFarmingEvent(jobTask, isFinalTask)
	}
}

func RpcRequestScanFarmingEvent(jobInfo common.ScheduledTaskRecord, isFinalTask bool) {
	nextCursor := ""
	if jobInfo.Digest != nil {
		nextCursor = *jobInfo.Digest
	}
	reqParams := rpcSdk.SuiTransactionBlockInputParameter(jobInfo.InputObjectId, nextCursor)
	resp := rpcSdk.EventRpcRequest(reqParams, "ScanFarmingEvent")
	common.EventsCursorUpdateById(resp.NextCursor, jobInfo.Id)
	if len(resp.Data) == 0 {
		log.Printf("RpcRequestScanFarmingEvent lastCursor=%v\n", nextCursor)
		if isFinalTask {
			common.UpdateTimingTypeExecutionCompleted(false, common.ScheduledTaskTypeFarmingPoolId, 0) // 更新所有Vault任务未执行
		} else {
			common.UpdateTimingTypeExecutionCompleted(true, common.ScheduledTaskTypeFarmingPoolId, jobInfo.Id) // 更新所有Vault任务已完成执行
		}
		return
	}
	for _, data := range resp.Data {
		digest := data.Digest
		transactionTimeUnix := data.TimestampMs
		for _, event := range data.Events {
			eventType := EventType(event.Type)
			switch eventType {
			// case FarmingPoolCreateEvent: // 创建FarmingPool
			// 	InsertFarmingPoolCreate(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingRewardConfigAddEvent: //奖励配置新增
				InsertFarmingRewardConfigAdd(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingRewardBankFundEvent: //奖励池充值
				InsertFarmingRewardBankFund(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingRewardBankExtractEvent: //奖励池提取
				InsertFarmingRewardBankExtract(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingRewardConfigUpdateEvent: //奖励配置更新
				InsertFarmingRewardConfigUpdate(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingPoolPauseEvent: //资金池暂停
				InsertFarmingPoolPause(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingPoolResumeEvent: //资金池恢复
				InsertFarmingPoolResume(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingStakeEvent: //质押操作
				InsertFarmingStake(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingUnstakeEvent: //解质押操作
				InsertFarmingUnstake(event.ParsedJson, digest, transactionTimeUnix)
			case FarmingClaimEvent: //奖励领取
				InsertFarmingClaim(event.ParsedJson, digest, transactionTimeUnix)
				// case FarmingRoleUpdateEvent: //角色权限更新
				// 	InsertFarmingRoleUpdate(event.ParsedJson, digest, transactionTimeUnix)
				// case FarmingMigrateEvent: //合约版本迁移
				// 	InsertFarmingMigrate(event.ParsedJson, digest, transactionTimeUnix)
			}
		}
	}
}

func InsertFarmingPoolCreate(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)
	stake_token_type := parsedJson["stake_token_type"].(map[string]interface{})["name"].(string)
	model := parsedJson["model"].(string)
	market_id := parsedJson["market_id"].(string)
	hearn_addr := parsedJson["hearn_addr"].(string)
	vault_addr := parsedJson["vault_addr"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_pool_create where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_pool_create查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_pool_create digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_pool_create(pool_id,caller,stake_token_type,model,market_id,hearn_addr,vault_addr,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, stake_token_type, model, market_id, hearn_addr, vault_addr, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_pool_create新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_pool_create新增id：=%v", lastInsertID)
	common.InsertScheduledTask(con, common.ScheduledTaskTypeFarmingPoolId, pool_id, "InsertFarmingPoolCreate")
	defer con.Close()
}

func InsertFarmingRewardConfigAdd(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)
	reward_bank_id := parsedJson["reward_bank_id"].(string)
	reward_token_type := parsedJson["reward_token_type"].(map[string]interface{})["name"].(string)
	start_time := parsedJson["start_time"].(string)
	reward_per_second := parsedJson["reward_per_second"].(string)
	end_time := parsedJson["end_time"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_reward_config_add where digest=? and pool_id=? and reward_token_type=?", digest, pool_id, reward_token_type)
	if queryErr != nil {
		log.Printf("farming_reward_config_add查询 digest+pool_id+reward_token_type失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_reward_config_add digest+pool_id+reward_token_type exist :%v,%v,%v\n", digest, pool_id, reward_token_type)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_reward_config_add(pool_id,caller,reward_bank_id,reward_token_type,start_time,reward_per_second,end_time,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, reward_bank_id, reward_token_type, start_time, reward_per_second, end_time, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_reward_config_add新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_reward_config_add新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingRewardBankFund(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)
	reward_bank_id := parsedJson["reward_bank_id"].(string)
	reward_token_type := parsedJson["reward_token_type"].(map[string]interface{})["name"].(string)
	amount := parsedJson["amount"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_reward_bank_fund where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_reward_bank_fund查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_reward_bank_fund digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_reward_bank_fund(pool_id,caller,reward_bank_id,reward_token_type,amount,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, reward_bank_id, reward_token_type, amount, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_reward_bank_fund新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_reward_bank_fund新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingRewardBankExtract(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)
	reward_bank_id := parsedJson["reward_bank_id"].(string)
	reward_token_type := parsedJson["reward_token_type"].(map[string]interface{})["name"].(string)
	amount := parsedJson["amount"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_reward_bank_extract where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_reward_bank_extract查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_reward_bank_extract digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_reward_bank_extract(pool_id,caller,reward_bank_id,reward_token_type,amount,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, reward_bank_id, reward_token_type, amount, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_reward_bank_extract新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_reward_bank_extract新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingRewardConfigUpdate(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)
	reward_bank_id := parsedJson["reward_bank_id"].(string)
	reward_token_type := parsedJson["reward_token_type"].(map[string]interface{})["name"].(string)
	start_time := parsedJson["start_time"].(string)
	reward_per_second := parsedJson["reward_per_second"].(string)
	end_time := parsedJson["end_time"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_reward_config_update where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_reward_config_update查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_reward_config_update digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_reward_config_update(pool_id,caller,reward_bank_id,reward_token_type,start_time,reward_per_second,end_time,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, reward_bank_id, reward_token_type, start_time, reward_per_second, end_time, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_reward_config_update新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_reward_config_update新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingPoolPause(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_pool_pause where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_pool_pause查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_pool_pause digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_pool_pause(pool_id,caller,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_pool_pause新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_pool_pause新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingPoolResume(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	caller := parsedJson["caller"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_pool_resume where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_pool_resume查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_pool_resume digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_pool_resume(pool_id,caller,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, caller, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_pool_resume新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_pool_resume新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingStake(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	user := parsedJson["user"].(string)
	amount := parsedJson["amount"].(string)
	total_shares := parsedJson["total_shares"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_stake where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_stake查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_stake digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_stake(pool_id,user,amount,total_shares,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, user, amount, total_shares, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_stake新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_stake新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingUnstake(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	user := parsedJson["user"].(string)
	amount := parsedJson["amount"].(string)
	total_shares := parsedJson["total_shares"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_unstake where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_unstake查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_unstake digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_unstake(pool_id,user,amount,total_shares,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, user, amount, total_shares, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_unstake新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_unstake新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingClaim(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	pool_id := parsedJson["pool_id"].(string)
	reward_token_type := parsedJson["reward_token_type"].(map[string]interface{})["name"].(string)
	user := parsedJson["user"].(string)
	amount := parsedJson["amount"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_claim where digest=? and pool_id=?", digest, pool_id)
	if queryErr != nil {
		log.Printf("farming_claim查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_claim digest+pool_id exist :%v,%v\n", digest, pool_id)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_claim(pool_id,reward_token_type,user,amount,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, pool_id, reward_token_type, user, amount, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_claim新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_claim新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingRoleUpdate(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller := parsedJson["caller"].(string)
	role := parsedJson["role"].(string)
	account := parsedJson["account"].(string)
	added := parsedJson["added"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_role_update where digest=? and caller=?", digest, caller)
	if queryErr != nil {
		log.Printf("farming_role_update查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_role_update digest+caller exist :%v,%v\n", digest, caller)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_role_update(caller,role,account,added,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, caller, role, account, added, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_role_update新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_role_update新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertFarmingMigrate(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller := parsedJson["caller"].(string)
	new_version := parsedJson["new_version"].(string)

	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", ttConvErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from farming_migrate where digest=? and caller=?", digest, caller)
	if queryErr != nil {
		log.Printf("farming_migrate查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		log.Printf("farming_migrate digest+caller exist :%v,%v\n", digest, caller)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into farming_migrate(caller,new_version,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, caller, new_version, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("farming_migrate新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("farming_migrate新增id：=%v", lastInsertID)
	defer con.Close()
}

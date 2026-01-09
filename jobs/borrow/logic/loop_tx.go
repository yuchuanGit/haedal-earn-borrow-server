package logic

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"haedal-earn-borrow-server/common"
	"haedal-earn-borrow-server/common/mydb"
	"haedal-earn-borrow-server/common/rpcSdk"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
)

const (
	CreateMarketEvent            = "::events::CreateMarketEvent"                       // 创建Market
	SupplyEvent                  = "::events::SupplyEvent"                             // 存数量
	SupplyCollateralEvent        = "::events::SupplyCollateralEvent"                   // 存抵押
	BorrowEvent                  = "::events::BorrowEvent"                             // 借数量
	RepayEvent                   = "::events::RepayEvent"                              // 还数量
	WithdrawEvent                = "::events::WithdrawEvent"                           // 取资产
	WithdrawCollateralEvent      = "::events::WithdrawCollateralEvent"                 // 取抵押
	AccrueInterestEvent          = "::events::AccrueInterestEvent"                     // 计利息
	BorrowRateUpdateEvent        = "::events::BorrowRateUpdateEvent"                   // 借款利率更新
	LiquidateEvent               = "::events::LiquidateEvent"                          //borrow清算
	VaultEvent                   = "::meta_vault_events::VaultEvent"                   // 创建Vault
	SetAllocationEvent           = "::meta_vault_events::SetAllocationEvent"           // 设置Borrow cap
	SetSupplyQueueEvent          = "::meta_vault_events::SetSupplyQueueEvent"          // 设置存款队列，Vault和Market相关联
	SetWithdrawQueueEvent        = "::meta_vault_events::SetWithdrawQueueEvent"        // 设置取款队列，Vault和Market相关联
	SetOwnerEvent                = "::meta_vault_events::SetOwnerEvent"                // Vault设置更新owner记录
	SetCuratorEvent              = "::meta_vault_events::SetCuratorEvent"              // Vault设置更新Curator记录
	SetAllocatorEvent            = "::meta_vault_events::SetAllocatorEvent"            // Vault设置更新Allocator记录
	SubmitTimelockEvent          = "::meta_vault_events::SubmitTimelockEvent"          // 提交时间锁记录
	SetGuardianEvent             = "::meta_vault_events::SetGuardianEvent"             // 设置更新Guardian记录
	RevokePendingEvent           = "::meta_vault_events::RevokePendingEvent"           // Vault撤销待定记录
	SubmitSupplyCapEvent         = "::meta_vault_events::SubmitSupplyCapEvent"         // vault提交生效cap
	ApplySupplyCapEvent          = "::meta_vault_events::ApplySupplyCapEvent"          // vault应用存入上限cap
	SubmitMarketRemovalEvent     = "::meta_vault_events::SubmitMarketRemovalEvent"     // vault提交移除Market
	RemoveMarketEvent            = "::meta_vault_events::RemoveMarketEvent"            // vault提交移除Market
	SetMinDepositEvent           = "::meta_vault_events::SetMinDepositEvent"           // vault设置最大押金
	SetMaxDepositEvent           = "::meta_vault_events::SetMaxDepositEvent"           // vault设置最小押金
	SetWithdrawCooldownEvent     = "::meta_vault_events::SetWithdrawCooldownEvent"     // vault设置提款冷却事件
	SetMinRebalanceIntervalEvent = "::meta_vault_events::SetMinRebalanceIntervalEvent" // 设置最小再平衡间隔
	UpdateLastRebalanceEvent     = "::meta_vault_events::UpdateLastRebalanceEvent"     // 更新上次重新平衡事件
	SetFeeRecipientEvent         = "::meta_vault_events::SetFeeRecipientEvent"         // 设置费用接收人
	SubmitPerformanceFeeEvent    = "::meta_vault_events::SubmitPerformanceFeeEvent"    // 提交绩效费
	ApplyPerformanceFeeEvent     = "::meta_vault_events::ApplyPerformanceFeeEvent"     // 申请绩效费
	SubmitManagementFeeEvent     = "::meta_vault_events::SubmitManagementFeeEvent"     // 提交管理费
	ApplyManagementFeeEvent      = "::meta_vault_events::ApplyManagementFeeEvent"      // 申请管理费
	VaultDepositEvent            = "::meta_vault_events::VaultDepositEvent"            // 用户存入Vault池
	VaultWithdrawEvent           = "::meta_vault_events::VaultWithdrawEvent"           // 用户取出Vault池
	SetVaultNameEvent            = "::meta_vault_events::SetVaultNameEvent"            // 设置Vault名称
	CompensateLostAssetsEvent    = "::meta_vault_events::CompensateLostAssetsEvent"    // Vault补偿损失资产
	AccrueFeesEvent              = "::meta_vault_events::AccrueFeesEvent"              // Vault池应计费用
	RebalanceEvent               = "::meta_vault_events::RebalanceEvent"               // Vault池Rebalance
)

// func SuiTransactionBlockInputParameter(InputObjectId string, nextCursor string) models.SuiXQueryTransactionBlocksRequest {
// 	params := models.SuiXQueryTransactionBlocksRequest{
// 		SuiTransactionBlockResponseQuery: models.SuiTransactionBlockResponseQuery{
// 			TransactionFilter: models.TransactionFilter{
// 				// "ChangedObject": HEarnObjectId,
// 				"InputObject": InputObjectId,
// 				// "MoveFunction": map[string]interface{}{
// 				// 	"package": PackageId,
// 				// 	"module":  "market_borrower_entry",
// 				// },
// 			},
// 			Options: models.SuiTransactionBlockOptions{
// 				ShowInput:   true,
// 				ShowEffects: true,
// 				ShowEvents:  true,
// 			},
// 		},
// 		Limit:           50,
// 		DescendingOrder: false,
// 	}
// 	if nextCursor != "" && nextCursor != "null" && nextCursor != "undefined" {
// 		params.Cursor = nextCursor
// 	}
// 	return params
// }

func RpcApiRequest(nextCursor string) {
	log.Printf("SuiXQueryTransactionBlocks nextCursor=%v\n", nextCursor)
	cli := sui.NewSuiClient(rpcSdk.SuiBlockvisionEnv)
	ctx := context.Background()
	resp, err := cli.SuiXQueryTransactionBlocks(ctx, rpcSdk.SuiTransactionBlockInputParameter(rpcSdk.HEarnObjectId, nextCursor))
	if err != nil {
		fmt.Printf("borrow RpcApiRequest err:%v\n", err)
		return
	}
	common.EventsCursorUpdate(resp.NextCursor, common.ScheduledTaskTypeBorrow)
	if len(resp.Data) == 0 {
		log.Printf("SuiXQueryTransactionBlocks lastCursor=%v\n", nextCursor)
		return
	}
	for i := len(resp.Data) - 1; i >= 0; i-- {
		data := resp.Data[i]
		digest := data.Digest
		transactionTime := data.TimestampMs
		transactions := data.Transaction.Data.Transaction.Transactions
		for _, event := range data.Events {
			// eventType := strings.Replace(event.Type, PackageId, "", 1)
			eventType := EventType(event.Type)
			switch eventType {
			case CreateMarketEvent:
				InsertBorrow(event.ParsedJson, digest, transactionTime)
			case SupplyEvent: // 存资产操作
				InsertBorrowSupplyDetali(event.ParsedJson, digest, transactionTime)
			case SupplyCollateralEvent: // 抵押存入
				InsertBorrowSupplyDetaliCollateral(event.ParsedJson, digest, transactionTime)
			case BorrowEvent: // 借
				InsertBorrowDetali(event.ParsedJson, digest, transactionTime)
			case RepayEvent: // 还
				InsertBorrowRepayDetali(event.ParsedJson, digest, transactionTime)
			case WithdrawEvent: // 取资产
				InsertBorroWithdraw(event.ParsedJson, digest, transactionTime)
			case WithdrawCollateralEvent: // 取抵押
				InsertBorroWithdrawCollateral(event.ParsedJson, digest, transactionTime)
			case AccrueInterestEvent: // 计利息
				InsertRateDetali(event.ParsedJson, transactions, digest, transactionTime)
			case BorrowRateUpdateEvent: //	借款利率更新
				InsertBorrowRateUpdate(event.ParsedJson, digest, transactionTime)
			case LiquidateEvent: //borrow清算
				InsertBorrowLiquidate(event.ParsedJson, digest, transactionTime)
			}
		}
	}
	RpcApiRequest(resp.NextCursor)
}

func InsertBorrowLiquidate(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	caller := parsedJson["caller"].(string)
	borrower := parsedJson["borrower"].(string)
	repaid_assets := parsedJson["repaid_assets"].(string)
	repaid_shares := parsedJson["repaid_shares"].(string)
	seized_assets := parsedJson["seized_assets"].(string)
	bad_debt_assets := parsedJson["bad_debt_assets"].(string)
	bad_debt_shares := parsedJson["bad_debt_shares"].(string)
	collateral_token_type := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loan_token_type := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_liquidate where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow_liquidate查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("LiquidateEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}

	sql := "insert into borrow_liquidate(market_id, caller, borrower, repaid_assets, repaid_shares,seized_assets, bad_debt_assets, bad_debt_shares, loan_token_type,collateral_token_type, digest, transaction_time_unix, transaction_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller, borrower, repaid_assets, repaid_shares,
		seized_assets, bad_debt_assets, bad_debt_shares, loan_token_type, collateral_token_type,
		digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_liquidate新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_liquidate新增id：=%v", lastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowRateUpdate(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	avg_borrow_rate := parsedJson["avg_borrow_rate"]
	rate_at_target := parsedJson["rate_at_target"]

	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_rate_update where digest=? and market_id=?", digest, market_id)
	if queryErr != nil {
		log.Printf("borrow_rate_update查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("BorrowRateUpdateEvent digest+market_id exist :%v,%v\n", digest, market_id)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}
	sql := "insert into borrow_rate_update(market_id,avg_borrow_rate,rate_at_target,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, avg_borrow_rate, rate_at_target, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_rate_update新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_rate_update新增id：=%v", lastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func GetTransactionFunctionName(transactions []interface{}) string {
	for _, transaction := range transactions {
		moveCall := models.MoveCall(transaction)
		if moveCall != nil { // 通过是否为nil判断是否转换成功
			return moveCall.Function
		}
	}
	return ""
}

func InsertRateDetali(parsedJson map[string]interface{}, transactions []interface{}, digest string, transactionTimeUnix string) {
	funcName := GetTransactionFunctionName(transactions)
	if funcName != "" {
		market_id := parsedJson["market_id"].(string)
		collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
		loanlType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
		borrowRate := parsedJson["borrow_rate"].(string)
		feeShares := parsedJson["fee_shares"].(string)
		interest := parsedJson["interest"].(string)
		convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
		if convErr != nil {
			log.Printf("转换失败：%v\n", convErr)
		}
		transactionTime := time.UnixMilli(convRs)
		con := mydb.GetDbConnection()
		queryRs, queryErr := con.Query("select * from rate_detail where digest=? and market_id=?", digest, market_id)
		if queryErr != nil {
			log.Printf("borrow查询 digest失败: %v", queryErr)
			defer con.Close()
			return
		}
		if queryRs.Next() {
			fmt.Printf("AccrueInterestEvent digest+market_id exist :%v,%v\n", digest, market_id)
			defer queryRs.Close()
			defer con.Close()
			return
		}
		sql := "insert into rate_detail(rate_type,market_id,collateral_token_type,loan_token_type,borrow_rate,fee_shares,interest,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
		result, err := con.Exec(sql, funcName, market_id, collateralType, loanlType, borrowRate, feeShares, interest, digest, transactionTimeUnix, transactionTime)
		if err != nil {
			log.Printf("rate_detail新增失败: %v", err)
			defer con.Close()
			return
		}
		lastInsertID, _ := result.LastInsertId()
		log.Printf("rate_detail新增id：=%v", lastInsertID)
		defer con.Close()
	}
}

func InsertBorroWithdraw(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	receiver := parsedJson["receiver"].(string)
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loanlType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_withdraw where digest=? and market_id=?", digest, market_id)
	if queryErr != nil {
		log.Printf("borrow_withdraw查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("WithdrawEvent digest+market_id exist :%v,%v\n", digest, market_id)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}
	sql := "insert into borrow_withdraw(market_id,caller,on_behalf,receiver,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	sqlDw := "insert into borrow_assets_operation_record(operation_type,market_id,caller,on_behalf,receiver,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, receiver, assets, shares, collateralType, loanlType, digest, transactionTimeUnix, transactionTime)
	resultDw, errDw := con.Exec(sqlDw, "Withdraw", market_id, caller_address, on_behalf_address, receiver, assets, shares, collateralType, loanlType, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_withdraw新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_withdraw新增id：=%v", lastInsertID)
	if errDw != nil {
		log.Printf("borrow_assets_operation_record 新增失败: %v", errDw)
		defer con.Close()
		return
	}
	dwLastInsertID, _ := resultDw.LastInsertId()
	log.Printf("borrow_assets_operation_record 新增id：=%v", dwLastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorroWithdrawCollateral(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	receiver_address := parsedJson["receiver"].(string)
	assets := parsedJson["assets"].(string)
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loanlType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_withdraw_collateral where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("RepayEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}
	sql := "insert into borrow_withdraw_collateral(market_id,caller_address,on_behalf_address,receiver,assets,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	sqlOr := "insert into borrow_assets_operation_record(operation_type,market_id,caller,on_behalf,receiver,assets,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, receiver_address, assets, collateralType, loanlType, digest, transactionTimeUnix, transactionTime)
	resultOr, errOr := con.Exec(sqlOr, "CollateralWithdraw", market_id, caller_address, on_behalf_address, receiver_address, assets, collateralType, loanlType, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_withdraw_collateral新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_withdraw_collateral新增id：=%v", lastInsertID)
	if errOr != nil {
		log.Printf("borrow_assets_operation_record 新增失败: %v", errOr)
		defer con.Close()
		return
	}
	orLastInsertID, _ := resultOr.LastInsertId()
	log.Printf("borrow_assets_operation_record 新增id：=%v", orLastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowRepayDetali(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
	collateral_token_type := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loan_token_type := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_repay_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("RepayEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}

	sql := "insert into borrow_repay_detail(market_id,caller_address,on_behalf_address,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	sqlOR := "insert into borrow_assets_operation_record(operation_type,market_id,caller,on_behalf,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, assets, shares, collateral_token_type, loan_token_type, digest, transactionTimeUnix, transactionTime)
	resultOR, errOR := con.Exec(sqlOR, "Repay", market_id, caller_address, on_behalf_address, assets, shares, collateral_token_type, loan_token_type, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_repay_detail新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_repay_detail新增id：=%v", lastInsertID)

	if errOR != nil {
		log.Printf("borrow_assets_operation_record 新增失败: %v", errOR)
		defer con.Close()
		return
	}
	orLastInsertID, _ := resultOR.LastInsertId()
	log.Printf("borrow_assets_operation_record 新增id：=%v", orLastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowDetali(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	receiver_address := parsedJson["receiver"].(string)
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
	collateral_token_type := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loan_token_type := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("BorrowEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}

	sql := "insert into borrow_detail(market_id,caller_address,on_behalf_address,receiver_address,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	sqlR := "insert into borrow_assets_operation_record(operation_type,market_id,caller,on_behalf,receiver,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, receiver_address, assets, shares, collateral_token_type, loan_token_type, digest, transactionTimeUnix, transactionTime)
	resultR, errR := con.Exec(sqlR, "Borrow", market_id, caller_address, on_behalf_address, receiver_address, assets, shares, collateral_token_type, loan_token_type, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_detail新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_detail新增id：=%v", lastInsertID)
	if errR != nil {
		log.Printf("borrow_assets_operation_record 新增失败: %v", errR)
		defer con.Close()
		return
	}
	dwLastInsertID, _ := resultR.LastInsertId()
	log.Printf("borrow_assets_operation_record 新增id：=%v", dwLastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowSupplyDetaliCollateral(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	market_id := parsedJson["market_id"].(string)
	assets := parsedJson["assets"].(string)
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loanType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	con := mydb.GetDbConnection()
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	queryRs, queryErr := con.Query("select * from borrow_supply_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("SupplyCollateralEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}

	sql := "insert into borrow_supply_detail(supply_type,market_id,caller_address,on_behalf_address,assets,digest,transaction_time_unix,transaction_time,collateral_token_type,loan_token_type) value(?,?,?,?,?,?,?,?,?,?)"
	sqlOr := "insert into borrow_assets_operation_record(operation_type,market_id,caller,on_behalf,assets,digest,transaction_time_unix,transaction_time,collateral_token_type,loan_token_type) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, 2, market_id, caller_address, on_behalf_address, assets, digest, transactionTimeUnix, transactionTime, collateralType, loanType)
	resultOr, errOr := con.Exec(sqlOr, "Collateral", market_id, caller_address, on_behalf_address, assets, digest, transactionTimeUnix, transactionTime, collateralType, loanType)
	if err != nil {
		log.Printf("borrow_supply_detail_collateral新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_detail_collateral新增id：=%v", lastInsertID)
	if errOr != nil {
		log.Printf("borrow_assets_operation_record 新增失败: %v", errOr)
		defer con.Close()
		return
	}
	orLastInsertID, _ := resultOr.LastInsertId()
	log.Printf("borrow_assets_operation_record 新增id：=%v", orLastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowSupplyDetali(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	market_id := parsedJson["market_id"].(string)
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loanType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := mydb.GetDbConnection()
	// tx, txErr := con.Begin()
	queryRs, queryErr := con.Query("select * from borrow_supply_detail where digest=? and market_id=?", digest, market_id)
	if queryErr != nil {
		log.Printf("borrow查询 digest+market_id失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("SupplyEvent digest+market_id exist :%v,%v\n", digest, market_id)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}

	sql := "insert into borrow_supply_detail(supply_type,market_id,caller_address,on_behalf_address,assets,shares,digest,transaction_time_unix,transaction_time,collateral_token_type,loan_token_type) value(?,?,?,?,?,?,?,?,?,?,?)"
	sqlDw := "insert into borrow_assets_operation_record(operation_type,market_id,caller,on_behalf,assets,shares,digest,transaction_time_unix,transaction_time,collateral_token_type,loan_token_type) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, 1, market_id, caller_address, on_behalf_address, assets, shares, digest, transactionTimeUnix, transactionTime, collateralType, loanType)
	resultDw, errDw := con.Exec(sqlDw, "Deposit", market_id, caller_address, on_behalf_address, assets, shares, digest, transactionTimeUnix, transactionTime, collateralType, loanType)
	if err != nil {
		log.Printf("borrow_supply_detail 新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_detail 新增id：=%v", lastInsertID)

	if errDw != nil {
		log.Printf("borrow_assets_operation_record 新增失败: %v", errDw)
		defer con.Close()
		return
	}
	dwLastInsertID, _ := resultDw.LastInsertId()
	log.Printf("borrow_assets_operation_record 新增id：=%v", dwLastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrow(parsedJson map[string]interface{}, digest string, transactionTime string) {
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	fee := parsedJson["fee"].(string)
	lltv := parsedJson["lltv"].(string)
	ltv := parsedJson["ltv"].(string)
	loanType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	marketId := parsedJson["market_id"].(string)
	oracleId := parsedJson["oracle_id"].(string)
	// titleBcs := parsedJson["title"].([]any)
	// titleBcsByte, _ := anyToBytes(titleBcs)
	// deserializer := bcs.NewDeserializer(titleBcsByte)
	// title := deserializer.ReadString()
	// title := ""
	baseTokenDecimals := parsedJson["base_token_decimals"].(string)
	quoteTokenDecimals := parsedJson["quote_token_decimals"].(string)

	// log.Printf("title=%v", title)

	con := mydb.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}

	if queryRs.Next() {
		fmt.Printf("CreateMarketEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集，避免资源泄漏
		defer con.Close()
		return
	}

	sql := "insert into borrow(digest,collateral_token_type,fee,lltv,ltv,loan_token_type,market_id,oracle_id,base_token_decimals,quote_token_decimals,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, digest, collateralType, fee, lltv, ltv, loanType, marketId, oracleId, baseTokenDecimals, quoteTokenDecimals, transactionTime)
	if err != nil {
		log.Printf("borrow 新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Println("borrow 新增id：=", lastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

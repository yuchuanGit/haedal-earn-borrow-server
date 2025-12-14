package logic

import (
	"context"
	"fmt"
	"haedal-earn-borrow-server/common"
	"log"
	"strconv"
	"time"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
)

const (
	// PackageId     = "0x7ba1b0d26e44022cf26e2e7f6b188495ddfb9c53cc6147723d2ff5deaff20bde" //11-26 16:00
	// HEarnObjectId = "0xb999d1d9d5d0f053a7572021d4e00618c729caf07f1d4a0a6c93164d32215a3f"
	// PackageId      = "0xd44cf630ff04308f3a08f161aa0378b71f399d644c3af58d6dcf4a58e3963e8c" //12-03
	// HEarnObjectId  = "0xfa50ba56632a857630cbfde7871f7b8b738227e971ebf6c46173f928a6880373"
	// PackageId      = "0x3279683b5157ce56a3d25a92b354750472053c31dd139771752d437ba8797906" //12-09 18:00
	// HEarnObjectId  = "0x42cbdc7214c46b55096fd76f9bed12382dd68216ec5147cd18624ff539dff04e"
	// PackageId      = "0x3746d5bebba155df504b352c16cf91f92d7b23042256be85338938b4c8fc8a53" //12-11 17:00
	// HEarnObjectId  = "0xc4cf77c655e2c6f0d9482bdff1a01094145fe0649b401d36ebccb8690dcbcb4c"
	PackageId      = "0xa6397390685ea1dbba886433d00cb26da2bbb986408dc6599eb18579d9e5a5b2" //12-12 14:00
	HEarnObjectId  = "0x491d7fd420ac60a7618484880a47865ea46165c861628c208318ad3774b7a6d8"
	SuiUserAddress = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
	SuiEnv         = "https://sui-testnet-endpoint.blockvision.org"
)

const (
	CreateMarketEvent            = "::events::CreateMarketEvent"                       //创建Market
	SupplyEvent                  = "::events::SupplyEvent"                             //存数量
	SupplyCollateralEvent        = "::events::SupplyCollateralEvent"                   //存抵押
	BorrowEvent                  = "::events::BorrowEvent"                             //借数量
	RepayEvent                   = "::events::RepayEvent"                              //还数量
	WithdrawEvent                = "::events::WithdrawEvent"                           //取资产
	WithdrawCollateralEvent      = "::events::WithdrawCollateralEvent"                 //取抵押
	AccrueInterestEvent          = "::events::AccrueInterestEvent"                     //计利息
	BorrowRateUpdateEvent        = "::events::BorrowRateUpdateEvent"                   //借款利率更新
	VaultEvent                   = "::meta_vault_events::VaultEvent"                   //创建Vault
	SetAllocationEvent           = "::meta_vault_events::SetAllocationEvent"           //设置Borrow cap
	SetSupplyQueueEvent          = "::meta_vault_events::SetSupplyQueueEvent"          //设置存款队列，Vault和Market相关联
	SetWithdrawQueueEvent        = "::meta_vault_events::SetWithdrawQueueEvent"        //设置取款队列，Vault和Market相关联
	SetCuratorEvent              = "::meta_vault_events::SetCuratorEvent"              //Vault设置更新Curator记录
	SetAllocatorEvent            = "::meta_vault_events::SetAllocatorEvent"            //Vault设置更新Allocator记录
	SubmitTimelockEvent          = "::meta_vault_events::SubmitTimelockEvent"          //提交时间锁记录
	SubmitGuardianEvent          = "::meta_vault_events::SubmitGuardianEvent"          //提交Guardian生效记录
	RevokePendingEvent           = "::meta_vault_events::RevokePendingEvent"           //Vault撤销待定记录
	SubmitSupplyCapEvent         = "::meta_vault_events::SubmitSupplyCapEvent"         //vault提交生效cap
	ApplySupplyCapEvent          = "::meta_vault_events::ApplySupplyCapEvent"          //vault应用存入上限cap
	SubmitMarketRemovalEvent     = "::meta_vault_events::SubmitMarketRemovalEvent"     //vault提交移除Market
	RemoveMarketEvent            = "::meta_vault_events::RemoveMarketEvent"            //vault提交移除Market
	SetMinDepositEvent           = "::meta_vault_events::SetMinDepositEvent"           //vault设置最大押金
	SetMaxDepositEvent           = "::meta_vault_events::SetMaxDepositEvent"           //vault设置最小押金
	SetWithdrawCooldownEvent     = "::meta_vault_events::SetWithdrawCooldownEvent"     //vault设置提款冷却事件
	SetMinRebalanceIntervalEvent = "::meta_vault_events::SetMinRebalanceIntervalEvent" //设置最小再平衡间隔
	UpdateLastRebalanceEvent     = "::meta_vault_events::UpdateLastRebalanceEvent"     //更新上次重新平衡事件
	SetFeeRecipientEvent         = "::meta_vault_events::SetFeeRecipientEvent"         //设置费用接收人
	SubmitPerformanceFeeEvent    = "::meta_vault_events::SubmitPerformanceFeeEvent"    //提交绩效费
	ApplyPerformanceFeeEvent     = "::meta_vault_events::ApplyPerformanceFeeEvent"     //申请绩效费
	SubmitManagementFeeEvent     = "::meta_vault_events::SubmitManagementFeeEvent"     //提交管理费
	ApplyManagementFeeEvent      = "::meta_vault_events::ApplyManagementFeeEvent"      //申请管理费
	VaultDepositEvent            = "::meta_vault_events::VaultDepositEvent"            //用户存入Vault池
	VaultWithdrawEvent           = "::meta_vault_events::VaultWithdrawEvent"           //用户取出Vault池
	SetVaultNameEvent            = "::meta_vault_events::SetVaultNameEvent"            //设置Vault名称
	CompensateLostAssetsEvent    = "::meta_vault_events::CompensateLostAssetsEvent"    //Vault补偿损失资产
	AccrueFeesEvent              = "::meta_vault_events::AccrueFeesEvent"              //Vault池应计费用
	RebalanceEvent               = "::meta_vault_events::RebalanceEvent"               //Vault池Rebalance
)

func SuiTransactionBlockParameter(nextCursor string) models.SuiXQueryTransactionBlocksRequest {
	params := models.SuiXQueryTransactionBlocksRequest{
		SuiTransactionBlockResponseQuery: models.SuiTransactionBlockResponseQuery{
			TransactionFilter: models.TransactionFilter{
				// "ChangedObject": HEarnObjectId,
				"InputObject": HEarnObjectId,
				// "MoveFunction": map[string]interface{}{
				// 	"package": PackageId,
				// 	"module":  "market_borrower_entry",
				// },
			},
			Options: models.SuiTransactionBlockOptions{
				ShowInput:   true,
				ShowEffects: true,
				ShowEvents:  true,
			},
		},
		Limit:           50,
		DescendingOrder: false,
	}
	if nextCursor != "" && nextCursor != "null" && nextCursor != "undefined" {
		params.Cursor = nextCursor
	}
	return params
}

func RpcApiRequest(nextCursor string) {
	log.Printf("SuiXQueryTransactionBlocks nextCursor=%v\n", nextCursor)
	cli := sui.NewSuiClient(SuiEnv)
	ctx := context.Background()
	resp, err := cli.SuiXQueryTransactionBlocks(ctx, SuiTransactionBlockParameter(nextCursor))
	if err != nil {
		fmt.Printf("RpcApiRequest err:%v\n", err)
		return
	}
	EventsCursorUpdate(resp.NextCursor)
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
			case SupplyEvent: //存操作
				InsertBorrowSupplyDetali(event.ParsedJson, digest, transactionTime)
			case SupplyCollateralEvent: //抵押存入
				InsertBorrowSupplyDetaliCollateral(event.ParsedJson, digest, transactionTime)
			case BorrowEvent: //借
				InsertBorrowDetali(event.ParsedJson, digest, transactionTime)
			case RepayEvent: //还
				InsertBorrowRepayDetali(event.ParsedJson, digest, transactionTime)
			case WithdrawEvent: //取资产
				InsertBorroWithdraw(event.ParsedJson, digest, transactionTime)
			case WithdrawCollateralEvent: // 取抵押
				InsertBorroWithdrawCollateral(event.ParsedJson, digest, transactionTime)
			case AccrueInterestEvent: //计利息
				InsertRateDetali(event.ParsedJson, transactions, digest, transactionTime)
			case BorrowRateUpdateEvent: //	借款利率更新
				InsertBorrowRateUpdate(event.ParsedJson, digest, transactionTime)
			}
		}
	}
	RpcApiRequest(resp.NextCursor)
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
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_rate_update where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow_rate_update查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("BorrowRateUpdateEvent digest exist :%v\n", digest)
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

func EventsCursorUpdate(digest string) {
	con := common.GetDbConnection()
	sql := "update scheduled_task_record set digest=? where timing_type=1"
	rs, err := con.Exec(sql, digest)
	if err != nil {
		log.Printf("scheduled_task_record update digest失败：%v\n", err)
	}
	updateRowCount, _ := rs.RowsAffected()
	log.Printf("scheduled_task_record updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func QueryEventsCursor() string {
	digest := ""
	con := common.GetDbConnection()
	sql := "select digest from scheduled_task_record where timing_type=1"
	err := con.QueryRow(sql).Scan(&digest)
	if err != nil {
		log.Printf("QueryEventsCursor失败：%v\n", err)
		defer con.Close()
		return digest
	}
	defer con.Close()
	return digest
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
		con := common.GetDbConnection()
		queryRs, queryErr := con.Query("select * from rate_detail where digest=?", digest)
		if queryErr != nil {
			log.Printf("borrow查询 digest失败: %v", queryErr)
			defer con.Close()
			return
		}
		if queryRs.Next() {
			fmt.Printf("AccrueInterestEvent digest exist :%v\n", digest)
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
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loanlType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_withdraw where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow_withdraw查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("WithdrawEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}
	sql := "insert into borrow_withdraw(market_id,caller,on_behalf,receiver,assets,shares,collateral_token_type,loan_token_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, assets, shares, collateralType, loanlType, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_withdraw新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_withdraw新增id：=%v", lastInsertID)
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
	con := common.GetDbConnection()
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
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, receiver_address, assets, collateralType, loanlType, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_withdraw_collateral新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_withdraw_collateral新增id：=%v", lastInsertID)
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
	con := common.GetDbConnection()
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
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, assets, shares, collateral_token_type, loan_token_type, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_repay_detail新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_repay_detail新增id：=%v", lastInsertID)
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
	con := common.GetDbConnection()
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
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, receiver_address, assets, shares, collateral_token_type, loan_token_type, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_detail新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_detail新增id：=%v", lastInsertID)

	// borrowRs, borrowErr := con.Query("select * from borrow where market_id=?", market_id)
	// if borrowErr != nil {
	// 	log.Printf("borrow查询market_id失败: %v", borrowErr)
	// 	return
	// }
	// if borrowRs.Next() {
	// 	log.Printf("borrow-market_id查询1")
	// 	upRs, upErr := con.Exec("update borrow set total_loan_amount=total_loan_amount+? where market_id=?", assets, market_id)
	// 	if upErr != nil {
	// 		log.Printf("borrow 更新失败: %v", upErr)
	// 		return
	// 	}
	// 	upRow, _ := upRs.RowsAffected()
	// 	log.Printf("borrow 更新条数: %v", upRow)
	// }
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowSupplyDetaliCollateral(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	market_id := parsedJson["market_id"].(string)
	assets := parsedJson["assets"].(string)
	collateralType := parsedJson["collateral_token_type"].(map[string]interface{})["name"].(string)
	loanType := parsedJson["loan_token_type"].(map[string]interface{})["name"].(string)
	con := common.GetDbConnection()
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
	result, err := con.Exec(sql, 2, market_id, caller_address, on_behalf_address, assets, digest, transactionTimeUnix, transactionTime, collateralType, loanType)
	if err != nil {
		log.Printf("borrow_supply_detail_collateral新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_detail_collateral新增id：=%v", lastInsertID)

	// borrowRs, borrowErr := con.Query("select * from borrow where market_id=?", market_id)
	// if borrowErr != nil {
	// 	log.Printf("borrow查询market_id失败: %v", borrowErr)
	// 	return
	// }
	// if borrowRs.Next() {
	// 	upRs, upErr := con.Exec("update borrow set total_supply_collateral_amount=total_supply_collateral_amount+? where market_id=?", assets, market_id)
	// 	if upErr != nil {
	// 		log.Printf("borrow 更新失败: %v", upErr)
	// 		return
	// 	}
	// 	upRow, _ := upRs.RowsAffected()
	// 	log.Printf("borrow 更新条数: %v", upRow)
	// }
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
	con := common.GetDbConnection()
	// tx, txErr := con.Begin()
	queryRs, queryErr := con.Query("select * from borrow_supply_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("SupplyEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}

	sql := "insert into borrow_supply_detail(supply_type,market_id,caller_address,on_behalf_address,assets,shares,digest,transaction_time_unix,transaction_time,collateral_token_type,loan_token_type) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, 1, market_id, caller_address, on_behalf_address, assets, shares, digest, transactionTimeUnix, transactionTime, collateralType, loanType)
	if err != nil {
		log.Printf("新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_detail新增id：=%v", lastInsertID)

	// borrowRs, borrowErr := con.Query("select * from borrow where market_id=?", market_id)
	// if borrowErr != nil {
	// 	log.Printf("borrow查询market_id失败: %v", borrowErr)
	// 	return
	// }
	// if borrowRs.Next() {
	// 	upRs, upErr := con.Exec("update borrow set total_supply_amount=total_supply_amount+? where market_id=?", assets, market_id)
	// 	if upErr != nil {
	// 		log.Printf("borrow 更新失败: %v", upErr)
	// 		return
	// 	}
	// 	upRow, _ := upRs.RowsAffected()
	// 	log.Printf("borrow 更新条数: %v", upRow)
	// }
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
	titleBcs := parsedJson["title"].([]any)
	titleBcsByte, _ := anyToBytes(titleBcs)
	deserializer := bcs.NewDeserializer(titleBcsByte)
	title := deserializer.ReadString()
	// title := ""
	baseTokenDecimals := parsedJson["base_token_decimals"].(string)
	quoteTokenDecimals := parsedJson["quote_token_decimals"].(string)

	log.Printf("title=%v", title)

	con := common.GetDbConnection()
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

	sql := "insert into borrow(digest,market_title,collateral_token_type,fee,lltv,ltv,loan_token_type,market_id,oracle_id,base_token_decimals,quote_token_decimals,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, digest, title, collateralType, fee, lltv, ltv, loanType, marketId, oracleId, baseTokenDecimals, quoteTokenDecimals, transactionTime)
	if err != nil {
		log.Printf("新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Println("新增id：=", lastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

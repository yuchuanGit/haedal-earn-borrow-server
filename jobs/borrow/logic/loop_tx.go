package logic

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"haedal-earn-borrow-server/common"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
)

const (
	// PackageId         = "0x3bb6c9ecec37aa47a6b7cc40ddfb014004d6495f6733243e34482838ea956a0e" //12-17 20:47
	// HEarnObjectId     = "0xa125076a0bc69578e7e9f60ba4a96b742604e02f1c3fc2f8994e9a3b37183bba"
	// OracleObjectId    = "0x78f30de7d853e3245f82eafe639c149316e989bb6a33e5b6346c577475f04bf3"
	// PackageId         = "0x74640585be1b236885fe461c18f9a31aedd78cf9444d6af4e63b065940e41cdc" // 12-19 11:15
	// HEarnObjectId     = "0x0a1be2504d6e5a23fea45692558baf0b2f68166448c6f70d80148979c0b10dbb"
	// OracleObjectId    = "0xc745cc48a1e67312e5cc637d9c54a85065a5b718d16a13afb3fbf025b0aed918"
	// PackageId         = "0xa192fea008b04b3627a125c7774de1364a5b4d4e59345f6602be21a5adfc920a" // 12-23 11:25
	// HEarnObjectId     = "0xa4e27805c7bc0587a7907cb20fad95a1925bab4fca022d3337510812d368f0f1"
	// OracleObjectId    = "0x83095db301ef05c51a5868806be87feb530b1ca333652f8adb61cdfbb3c8dceb"
	PackageId         = "0xfedd869c200a819d4d8a440b29a082dbf227fd773fb4d88bf34df53435f005b4" // 12-29 15:13
	HEarnObjectId     = "0xa86275de9aa81366d14599ee3fe65c1de9c716e85a917a0a5b2e2dfa8fd95d18"
	OracleObjectId    = "0x155b1743460d76503bee9d7b3f1d686ebe3bcd572eeb200c257ddb8a8a4dc81d"
	FarmingObjectId   = "0x035f5a1b09ea99927a2dd811742b10cd456be323200bc1fdda6e17bbfef65cf7"
	SuiUserAddress    = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
	SuiBlockvisionEnv = "https://sui-testnet-endpoint.blockvision.org"
	SuiEnv            = "https://fullnode.testnet.sui.io:443"
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
	cli := sui.NewSuiClient(SuiBlockvisionEnv)
	ctx := context.Background()
	resp, err := cli.SuiXQueryTransactionBlocks(ctx, SuiTransactionBlockParameter(nextCursor))
	if err != nil {
		fmt.Printf("borrow RpcApiRequest err:%v\n", err)
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
	// digest := ""
	var digest sql.NullString
	con := common.GetDbConnection()
	sql := "select digest from scheduled_task_record where timing_type=1"
	err := con.QueryRow(sql).Scan(&digest)
	if err != nil {
		log.Printf("QueryEventsCursor失败：%v\n", err)
		defer con.Close()
		return ""
	}
	defer con.Close()
	if digest.Valid {
		return digest.String
	}
	return ""
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
	con := common.GetDbConnection()
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
	con := common.GetDbConnection()
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

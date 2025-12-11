package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"haedal-earn-borrow-server/common"
	"log"
	"strconv"
	"time"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/utils"
)

const (
	// PackageId     = "0x7ba1b0d26e44022cf26e2e7f6b188495ddfb9c53cc6147723d2ff5deaff20bde" //11-26 16:00
	// HEarnObjectId = "0xb999d1d9d5d0f053a7572021d4e00618c729caf07f1d4a0a6c93164d32215a3f"
	// PackageId      = "0xd44cf630ff04308f3a08f161aa0378b71f399d644c3af58d6dcf4a58e3963e8c" //12-03
	// HEarnObjectId  = "0xfa50ba56632a857630cbfde7871f7b8b738227e971ebf6c46173f928a6880373"
	PackageId      = "0x3279683b5157ce56a3d25a92b354750472053c31dd139771752d437ba8797906" //12-09 18:00
	HEarnObjectId  = "0x42cbdc7214c46b55096fd76f9bed12382dd68216ec5147cd18624ff539dff04e"
	SuiUserAddress = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
	SuiEnv         = "https://sui-testnet-endpoint.blockvision.org"
)

const (
	CreateMarketEvent            = "::events::CreateMarketEvent"       //创建Market
	SupplyEvent                  = "::events::SupplyEvent"             //存数量
	SupplyCollateralEvent        = "::events::SupplyCollateralEvent"   //存抵押
	BorrowEvent                  = "::events::BorrowEvent"             //借数量
	RepayEvent                   = "::events::RepayEvent"              //还数量
	WithdrawEvent                = "::events::WithdrawEvent"           //取资产
	WithdrawCollateralEvent      = "::events::WithdrawCollateralEvent" //取抵押
	AccrueInterestEvent          = "::events::AccrueInterestEvent"
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
			}
		}
	}
	RpcApiRequest(resp.NextCursor)
}

func InsertVaultRebalance(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	total_assets_before := parsedJson["total_assets_before"].(string)
	total_assets_after := parsedJson["total_assets_after"].(string)
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_rebalance where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_rebalance查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_rebalance digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_rebalance(vault_id,caller,total_assets_before,total_assets_after,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, total_assets_before, total_assets_after, asset_type, htoken_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_rebalance新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_rebalance新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultAccrueFees(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	management_fee_shares := parsedJson["management_fee_shares"].(string)
	performance_fee_shares := parsedJson["performance_fee_shares"].(string)
	total_shares_minted := parsedJson["total_shares_minted"].(string)
	total_assets := parsedJson["total_assets"].(string)
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_accrue_fees where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_accrue_fees查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_accrue_fees digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_accrue_fees(vault_id,management_fee_shares,performance_fee_shares,total_shares_minted,total_assets,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, management_fee_shares, performance_fee_shares, total_shares_minted, total_assets, asset_type, htoken_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_accrue_fees新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_accrue_fees新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultCompensateLostAssets(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	amount := parsedJson["amount"].(string)
	remaining_lost := parsedJson["remaining_lost"].(string)
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_compensate_lost_assets where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_compensate_lost_assets查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_compensate_lost_assets digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_compensate_lost_assets(vault_id,caller,amount,remaining_lost,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, amount, remaining_lost, asset_type, htoken_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_compensate_lost_assets新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_compensate_lost_assets新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetName(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	old_name := parsedJson["old_name"].(string)
	new_name := parsedJson["new_name"].(string)
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_name where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_name查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_name digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_name(vault_id,caller,old_name,new_name,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, old_name, new_name, asset_type, htoken_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_name新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_name新增id：=%v", lastInsertID)
	//todo vault 表更新最新名称
	defer con.Close()
}

func InsertVaultWithdraw(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	user := parsedJson["user"].(string)
	asset_amount := parsedJson["asset_amount"].(string)
	shares_burned := parsedJson["shares_burned"].(string)
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_withdraw where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_withdraw查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_withdraw digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_withdraw(vault_id,user,asset_amount,shares_burned,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, user, asset_amount, shares_burned, asset_type, htoken_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_withdraw新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_withdraw新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultDeposit(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	user := parsedJson["user"].(string)
	asset_amount := parsedJson["asset_amount"].(string)
	shares_minted := parsedJson["shares_minted"].(string)
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_deposit where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_deposit查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_deposit digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_deposit(vault_id,user,asset_amount,shares_minted,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, user, asset_amount, shares_minted, asset_type, htoken_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_deposit新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_deposit新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitmentFee(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	event_type := parsedJson["event_type"]
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_submit_management_fee where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_submit_management_fee查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_submit_management_fee digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_submit_management_fee(vault_id,caller,fee_bps,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, fee_bps, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_submit_management_fee新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_management_fee新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitPerformanceFee(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	event_type := parsedJson["event_type"]
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_submit_performance_fee where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_submit_performance_fee查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_submit_performance_fee digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_submit_performance_fee(vault_id,caller,fee_bps,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, fee_bps, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_submit_performance_fee新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_performance_fee新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultApplyManagementFee(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_apply_management_fee where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_apply_management_fee查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_apply_management_fee digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_apply_management_fee(vault_id,caller,fee_bps,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, fee_bps, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_apply_management_fee新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apply_management_fee新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultApplyPerformanceFee(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_apply_performance_fee where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_apply_performance_fee查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_apply_performance_fee digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_apply_performance_fee(vault_id,caller,fee_bps,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, fee_bps, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_apply_performance_fee新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apply_performance_fee新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetFeeRecipient(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	previous_fee_recipient := parsedJson["previous_fee_recipient"].(string)
	new_fee_recipient := parsedJson["new_fee_recipient"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_fee_recipient where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_fee_recipient查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_fee_recipient digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_fee_recipient(vault_id,caller,previous_fee_recipient,new_fee_recipient,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_fee_recipient, new_fee_recipient, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_fee_recipient新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_fee_recipient新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultUpdateLastRebalance(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	rebalance_time := parsedJson["rebalance_time"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_updatelast_rebalance where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_updatelast_rebalance查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_updatelast_rebalance digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_updatelast_rebalance(vault_id,caller,rebalance_time,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, rebalance_time, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_updatelast_rebalance新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_updatelast_rebalance新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetMinRebalanceInterval(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	interval := parsedJson["interval"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_min_rebalance_interval where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_min_rebalance_interval查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_min_rebalance_interval digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_min_rebalance_interval(vault_id,caller,interval,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, interval, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_min_rebalance_interval新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_min_rebalance_interval新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetWithdrawCooldown(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	cooldown_ms := parsedJson["cooldown_ms"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_withdraw_cooldown where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_withdraw_cooldown查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_withdraw_cooldown digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_withdraw_cooldown(vault_id,caller,cooldown_ms,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, cooldown_ms, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_withdraw_cooldown新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_withdraw_cooldown新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetMaxDeposit(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	max_deposit := parsedJson["max_deposit"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_max_deposit where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_max_deposit查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_max_deposit digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_max_deposit(vault_id,caller,max_deposit,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, max_deposit, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_max_deposit新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_max_deposit新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetMinDeposit(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	min_deposit := parsedJson["min_deposit"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_min_deposit where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_min_deposit查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_min_deposit digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_min_deposit(vault_id,caller,min_deposit,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, min_deposit, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_min_deposit新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_min_deposit新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitSupplyCap(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	new_cap := parsedJson["new_cap"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	event_type := parsedJson["event_type"]
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_submit_supply_cap where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_submit_supply_cap查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_submit_supply_cap digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_submit_supply_cap(vault_id,caller,new_cap,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, new_cap, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_submit_supply_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_supply_cap新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultApplySupplyCap(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	new_cap := parsedJson["new_cap"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_apply_supply_cap where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_apply_supply_cap查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_apply_supply_cap digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_apply_supply_cap(vault_id,caller,new_cap,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, new_cap, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_apply_supply_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apply_supply_cap新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitMarketRemoval(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	market_id := parsedJson["market_id"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	event_type := parsedJson["event_type"]
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_submit_market_removal where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_submit_market_removal查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_submit_market_removal digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_submit_market_removal(vault_id,caller,market_id,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, market_id, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_submit_market_removal新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_market_removal新增id：=%v", lastInsertID)
	defer con.Close()
}
func InsertVaultRemoveMarket(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	market_id := parsedJson["market_id"].(string)
	is_emergency := parsedJson["is_emergency"].(bool)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_remove_market where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_remove_market查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_remove_market digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_remove_market(vault_id,caller,market_id,is_emergency,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, market_id, is_emergency, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_remove_market新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_remove_market新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultRevokePending(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	pending_type := parsedJson["pending_type"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_revoke_pending where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_revoke_pending查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_revoke_pending digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_revoke_pending(vault_id,caller,pending_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, pending_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_revoke_pending新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_revoke_pending新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitGuardian(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	guardian := parsedJson["guardian"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	event_type := parsedJson["event_type"]
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_submit_guardian where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_submit_guardian查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_submit_guardian digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_submit_guardian(vault_id,caller,guardian,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, guardian, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_submit_guardian新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_guardian新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitTimeLock(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	new_timelock_minutes := parsedJson["new_timelock_minutes"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	event_type := parsedJson["event_type"]
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_submit_time_lock where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_submit_time_lock查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_submit_time_lock digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_submit_time_lock(vault_id,caller,new_timelock_minutes,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, new_timelock_minutes, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_submit_time_lock新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_time_lock新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetAllocatorRecord(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	previous_allocator := parsedJson["previous_allocator"].(string)
	new_allocator := parsedJson["new_allocator"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_allocator_record where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_allocator_record查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_allocator_record digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_allocator_record(vault_id,caller,previous_allocator,new_allocator,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_allocator, new_allocator, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_allocator_record新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_allocator_record新增id：=%v", lastInsertID)
	//todo 更新 vault主表allocator
	defer con.Close()
}

func InsertVaultSetCuratorRecord(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	previous_curator := parsedJson["previous_curator"].(string)
	new_curator := parsedJson["new_curator"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_curator_record where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_curator_record查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_curator_record digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_curator_record(vault_id,caller,previous_curator,new_curator,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_curator, new_curator, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_curator_record新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_curator_record新增id：=%v", lastInsertID)
	//todo 更新 vault主表curator
	defer con.Close()
}

func InsertWithdrawSupplyQueue(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	queue := parsedJson["queue"].([]interface{})
	queueJson, err := json.Marshal(queue)
	if err != nil {
		log.Fatalf("queue序列化为JSON失败: %v", err)
	}
	queueStr := string(queueJson) // 转为string类型，用于入库
	utils.PrettyPrint(queueStr)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_withdraw_queue where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_withdraw_queue查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vvault_withdraw_queue digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_withdraw_queue(vault_id,caller,queue,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, caller, vaultId, queueStr, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_allocation_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_withdraw_queue新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSupplyQueue(parsedJson map[string]interface{}, digest string) {
	utils.PrettyPrint(parsedJson)
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	queue := parsedJson["queue"].([]interface{})
	queueJson, err := json.Marshal(queue)
	if err != nil {
		log.Fatalf("queue序列化为JSON失败: %v", err)
	}
	queueStr := string(queueJson) // 转为string类型，用于入库
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_supply_queue where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_supply_queue查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_supply_queue digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_supply_queue(vault_id,caller,queue,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, caller, vaultId, queueStr, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_supply_queue新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_supply_queue新增id：=%v", lastInsertID)
	defer con.Close()
}

func SetAllocationCap(parsedJson map[string]interface{}, digest string) {
	vaultId := parsedJson["vault_id"].(string)
	marketId := parsedJson["market_id"].(string)
	caller := parsedJson["caller"].(string)
	cap := parsedJson["cap"].(string)
	weightBps := parsedJson["weight_bps"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_allocation_cap where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault_set_allocation_cap查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("SetAllocationCapEvent digest exist :%v\n", digest)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_allocation_cap(market_id,vault_id,cap,weight_bps,caller,timestamp_ms_unix,timestamp_ms,digest) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, marketId, vaultId, cap, weightBps, caller, timestampMsUnix, timestampMs, digest)
	if err != nil {
		log.Printf("vault_set_allocation_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_allocation_cap新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVault(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vault_id := parsedJson["vault_id"].(string)
	owner := parsedJson["owner"].(string)
	curator := parsedJson["curator"].(string)
	allocator := parsedJson["allocator"].(string)
	guardian := parsedJson["guardian"].(string)
	asset_decimals := parsedJson["asset_decimals"]
	asset_type := parsedJson["asset_type"].(map[string]interface{})["name"].(string)
	htoken_type := parsedJson["htoken_type"].(map[string]interface{})["name"].(string)

	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault where digest=?", digest)
	if queryErr != nil {
		log.Printf("vault查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("VaultEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		defer con.Close()
		return
	}
	sql := "insert into vault(vault_id,owner,curator,allocator,guardian,asset_decimals,asset_type,htoken_type,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vault_id, owner, curator, allocator, guardian, asset_decimals, asset_type, htoken_type, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault新增id：=%v", lastInsertID)
	isInsertTask := true
	taskRs, taskErr := con.Query("select * from scheduled_task_record where input_object_id=?", vault_id)
	if taskErr != nil {
		log.Printf("scheduled_task_record查询 input_object_id失败: %v", taskErr)
		defer con.Close()
		return
	}
	if taskRs.Next() {
		isInsertTask = false
	}
	if isInsertTask {
		sqlTask := "insert into scheduled_task_record(timing_type,input_object_id,execution_completed)"
		resultTask, errTask := con.Exec(sqlTask, 2, vault_id, 0)
		if errTask != nil {
			log.Printf("VaultEvent scheduled_task_record 新增失败: %v", errTask)
			defer con.Close()
			return
		}
		taskLastInsertID, _ := resultTask.LastInsertId()
		log.Printf("scheduled_task_record新增id：=%v", taskLastInsertID)
	}
	defer con.Close()
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

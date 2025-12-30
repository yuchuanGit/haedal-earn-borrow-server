package statistics

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"haedal-earn-borrow-server/common/mydb"
	"haedal-earn-borrow-server/common/rpcSdk"
	"haedal-earn-borrow-server/jobs/borrow/logic"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
)

func EarnTimedCollection() {
	vaultInfos := logic.QueryVaultAll()
	if len(vaultInfos) > 0 {
		transaction_time := time.Now()
		transaction_time = transaction_time.Add(-5 * time.Second) //当前时间5秒前
		for _, vaultInfo := range vaultInfos {
			EarnTvlTimedCollection(vaultInfo, transaction_time, 2)
		}
	}
}

func EarnTvlTimedCollection(vaultInfo logic.VaultModel, transaction_time time.Time, failRetryCount int) {
	cli := sui.NewSuiClient(rpcSdk.SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(rpcSdk.SuiUserAddress))
	arguments, parameErr := logic.GetYieldEarnedParameter(cli, ctx, *tx, vaultInfo.VaultId)
	typeArguments, typeErr := logic.AssetAndHTokenEnumsType(vaultInfo)
	if parameErr != nil {
		log.Printf("EarnTvlTimedCollection  parameErr:%v", parameErr.Error())
		EarnTvlTimedCollectionFailRetry(vaultInfo, transaction_time, failRetryCount)
		return
	}
	if typeErr != nil {
		log.Printf("EarnTvlTimedCollection  typeErr:%v", typeErr.Error())
		EarnTvlTimedCollectionFailRetry(vaultInfo, transaction_time, failRetryCount)
		return
	}
	moduleName := "meta_vault_core"
	funcName := "get_total_assets"
	moveCallReturn := logic.ExecuteDevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, typeArguments, arguments)
	if len(moveCallReturn) > 0 {
		tvl := "-1"
		for _, returnValue := range moveCallReturn[0].ReturnValues {
			bcsBytes, _ := logic.AnyToBytes(returnValue.([]any)[0])
			deserializer := bcs.NewDeserializer(bcsBytes)
			val := deserializer.U128()
			tvl = val.String()
		}
		if tvl != "-1" {
			log.Printf("VaultId=%v,tvl=%v\n", vaultInfo.VaultId, tvl)
			InsertVaultTvl(vaultInfo, tvl, transaction_time)
			InsertVaultYieldEarned(vaultInfo, tvl, transaction_time)
		}
	} else {
		EarnTvlTimedCollectionFailRetry(vaultInfo, transaction_time, failRetryCount)
	}
}

func EarnTvlTimedCollectionFailRetry(vaultInfo logic.VaultModel, transaction_time time.Time, failRetryCount int) {
	if failRetryCount > 0 {
		failRetryCount = failRetryCount - 1
		EarnTvlTimedCollection(vaultInfo, transaction_time, failRetryCount)
	}
}

func InsertVaultYieldEarned(vaultInfo logic.VaultModel, tvl string, transaction_time time.Time) {
	depositWithdrawAsset := 0.00
	handlingFee := 0.00
	con := mydb.GetDbConnection()
	sqlDW := "SELECT (SELECT ifnull(sum(asset_amount),0) deposit_asset from vault_deposit where vault_id=?)" +
		"-(SELECT ifnull(sum(asset_amount),0) withdraw_asset from vault_withdraw where vault_id=?) deposit_withdraw_asset"
	sqlFee := "SELECT ifnull(sum(management_fee_assets),0)+ifnull(sum(performance_fee_assets),0) as handling_fee from vault_accrue_fees where vault_id=?"
	errDW := con.QueryRow(sqlDW, vaultInfo.VaultId, vaultInfo.VaultId).Scan(&depositWithdrawAsset)
	errFee := con.QueryRow(sqlFee, vaultInfo.VaultId).Scan(&handlingFee)
	if errDW != nil {
		log.Printf("InsertVaultYieldEarned 查询本金存取净值失败: %v", errDW)
		defer con.Close()
		return
	}
	if errFee != nil {
		log.Printf("InsertVaultYieldEarned 查询管理费失败: %v", errFee)
		defer con.Close()
		return
	}
	tvlF, _ := strconv.ParseFloat(tvl, 64)
	earned_asset := tvlF - depositWithdrawAsset - handlingFee //总资产(本金+利息)-存/取净值本金-管理费(管理费+绩效费)
	deposit_withdraw_asset_str := fmt.Sprintf("%.0f", depositWithdrawAsset)
	handling_fee_str := fmt.Sprintf("%.0f", handlingFee)
	earned_asset_str := fmt.Sprintf("%.0f", earned_asset)
	transaction_time_unix := transaction_time.UnixMilli() // 毫秒
	sql := "insert into vault_yield_earned_record(vault_id,tvl,deposit_withdraw_asset,handling_fee,earned_asset,asset_type,asset_decimals,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultInfo.VaultId, tvl, deposit_withdraw_asset_str, handling_fee_str, earned_asset_str, vaultInfo.AssetType, vaultInfo.AssetDecimals, transaction_time_unix, transaction_time)
	if err != nil {
		log.Printf("vault_yield_earned_record 新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_yield_earned_record新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultTvl(vaultInfo logic.VaultModel, tvl string, transaction_time time.Time) {
	con := mydb.GetDbConnection()
	transaction_time_unix := transaction_time.UnixMilli() // 毫秒
	sql := "insert into vault_tvl_record(vault_id,total_asset,asset_type,asset_decimals,transaction_time_unix,transaction_time) value(?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultInfo.VaultId, tvl, vaultInfo.AssetType, vaultInfo.AssetDecimals, transaction_time_unix, transaction_time)
	if err != nil {
		log.Printf("vault_tvl_record 新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_tvl_record新增id：=%v", lastInsertID)
	defer con.Close()
}

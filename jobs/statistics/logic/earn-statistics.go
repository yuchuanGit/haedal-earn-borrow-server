package statistics

import (
	"context"
	"log"
	"time"

	"haedal-earn-borrow-server/common"
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
			log.Printf("VaultId=%v\n", vaultInfo.VaultId)
			EarnTvlTimedCollection(vaultInfo, transaction_time, 2)
		}
	}
}

func EarnTvlTimedCollection(vaultInfo logic.VaultModel, transaction_time time.Time, failRetryCount int) {
	cli := sui.NewSuiClient(logic.SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(logic.SuiUserAddress))
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

func InsertVaultTvl(vaultInfo logic.VaultModel, tvl string, transaction_time time.Time) {
	con := common.GetDbConnection()
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

package logic

import (
	"context"
	"haedal-earn-borrow-server/common"
	"log"
	"strings"
	"time"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
)

func VaultAllLoopExecuteMove() {
	vaultInfos := QueryVaultAll()
	if len(vaultInfos) > 0 {
		for _, vaultInfo := range vaultInfos {
			ExecuteMoveInsertVaultYieldEarned(vaultInfo, 2)
		}
	}
}

func ExecuteMoveInsertVaultYieldEarned(vaultInfo VaultModel, failRetryCount int) {
	cli := sui.NewSuiClient(SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(SuiUserAddress))
	arguments, parameErr := GetYieldEarnedParameter(cli, ctx, *tx, vaultInfo.VaultId)
	if parameErr != nil {
		ExecuteMoveUpdateVaultYieldEarnedFailRetry(vaultInfo, failRetryCount)
		return
	}
	assetParts := strings.Split(vaultInfo.AssetType, "::")
	if len(assetParts) < 3 {
		log.Printf("invalid vault AssetType type string: %v\n", vaultInfo.AssetType)
		return
	}
	htokenParts := strings.Split(vaultInfo.HtokenType, "::")
	if len(htokenParts) < 3 {
		log.Printf("invalid vault HtokenType type string: %v\n", vaultInfo.HtokenType)
		return
	}

	assetAddressBytes, _ := transaction.ConvertSuiAddressStringToBytes(models.SuiAddress(assetParts[0]))
	hTokenAddressBytes, _ := transaction.ConvertSuiAddressStringToBytes(models.SuiAddress(htokenParts[0]))
	typeArguments := []transaction.TypeTag{{
		Struct: &transaction.StructTag{
			Address: *assetAddressBytes,
			Module:  assetParts[1],
			Name:    assetParts[2],
		},
	}, {
		Struct: &transaction.StructTag{
			Address: *hTokenAddressBytes,
			Module:  htokenParts[1],
			Name:    htokenParts[2],
		},
	}}

	moduleName := "meta_vault_core"
	funcName := "get_exchange_rate"
	moveCallReturn := ExecuteDevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, typeArguments, arguments)
	if len(moveCallReturn) > 0 {
		exchangeRate := "0"
		for _, returnValue := range moveCallReturn[0].ReturnValues {
			bcsBytes, _ := anyToBytes(returnValue.([]any)[0])
			deserializer := bcs.NewDeserializer(bcsBytes)
			val := deserializer.U128()
			exchangeRate = val.String()
		}
		if exchangeRate != "0" {
			InsertVaultExchangeRate(vaultInfo.VaultId, exchangeRate)
		}
	} else {
		ExecuteMoveUpdateVaultYieldEarnedFailRetry(vaultInfo, failRetryCount)
	}
}

func ExecuteMoveUpdateVaultYieldEarnedFailRetry(vaultInfo VaultModel, failRetryCount int) {
	if failRetryCount > 0 {
		failRetryCount = failRetryCount - 1
		ExecuteMoveInsertVaultYieldEarned(vaultInfo, failRetryCount)
	}
}

func InsertVaultExchangeRate(vaultId string, exchangeRate string) {
	con := common.GetDbConnection()
	sql := "insert into vault_exchange_rate(vault_id,exchange_rate,transaction_time) value(?,?,?)"
	result, err := con.Exec(sql, vaultId, exchangeRate, time.Now())
	if err != nil {
		log.Printf("vault_exchange_rate新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_exchange_rate新增id：=%v", lastInsertID)
	defer con.Close()
}

func GetYieldEarnedParameter(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, vaultObjectId string) ([]transaction.Argument, error) {
	valutSharedObject, err := GetSharedObjectRef(ctx, cli, vaultObjectId, true)
	hearnSharedObject, err2 := GetSharedObjectRef(ctx, cli, HEarnObjectId, true)
	if err != nil {
		log.Printf("valutSharedObject fail:%v", err.Error())
		return nil, err
	}
	if err2 != nil {
		log.Printf("hearnSharedObject fail:%v", err2.Error())
		return nil, err2
	}

	arguments := []transaction.Argument{
		// 第一个参数：&Vault<Asset, HToken> 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					// 非共享对象用 ImmOrOwnedObject
					// ImmOrOwnedObject: &transaction.SuiObjectRef{ObjectId: *objectIdBytes, Version: ver, Digest: *digestBytes},
					SharedObject: valutSharedObject,
				},
			},
		),
		// 第二个参数：&Hear 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: hearnSharedObject,
				},
			},
		),
	}
	return arguments, err
}

func QueryVaultAll() []VaultModel {
	var vms []VaultModel
	con := common.GetDbConnection()
	sql := "select vault_id,vault_name,asset_type,htoken_type from vault "
	rs, err := con.Query(sql)
	if err != nil {
		log.Printf("QueryVaultAll 查询失败: %v", err)
		defer con.Close()
		return vms
	}
	for rs.Next() {
		var vm VaultModel
		errScan := rs.Scan(&vm.VaultId, &vm.VaultName, &vm.AssetType, &vm.HtokenType)
		if errScan != nil {
			log.Printf("QueryVaultAll scan失败: %v", err)
			defer con.Close()
			return vms
		}
		vms = append(vms, vm)
	}
	defer con.Close()
	return vms
}

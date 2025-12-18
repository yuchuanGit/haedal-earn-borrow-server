package logic

import (
	"context"
	"database/sql"
	"fmt"
	"haedal-earn-borrow-server/common"
	"log"
	"math"
	"strconv"
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
			InsertVaultExchangeRate(vaultInfo.VaultId, vaultInfo.AssetDecimals, exchangeRate)
			InsertVaultApy(vaultInfo.VaultId, exchangeRate)
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

func InsertVaultExchangeRate(vaultId string, asset_decimals float64, exchangeRate string) {
	con := common.GetDbConnection()
	sql := "insert into vault_exchange_rate(vault_id,exchange_rate,transaction_time) value(?,?,?)"
	exchangeRateF, _ := strconv.ParseFloat(exchangeRate, 64)
	powResult := math.Pow(10, asset_decimals+6)
	val := powResult / exchangeRateF
	result, err := con.Exec(sql, vaultId, fmt.Sprintf("%.0f", val), time.Now())
	if err != nil {
		log.Printf("vault_exchange_rate新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_exchange_rate新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultApy(vaultId string, exchangeRate string) {
	var preApy VaultApyModel
	con := common.GetDbConnection()
	querySql := "SELECT id,vault_id,exchange_rate,apy,transaction_time from vault_apy where  vault_id=? and transaction_time=(SELECT max(transaction_time) previousTime from vault_apy where vault_id=?)"
	queryErr := con.QueryRow(querySql, vaultId, vaultId).Scan(&preApy.Id, &preApy.VaultId, &preApy.ExchangeRate, &preApy.Apy, &preApy.TransactionTime)
	exchangeRateF, _ := strconv.ParseFloat(exchangeRate, 64)
	currentNow := time.Now()
	yearApy := 0.00
	if QueryRowIsValue(queryErr) {
		preExchangeRateF, _ := strconv.ParseFloat(preApy.ExchangeRate, 64)
		differenceInSeconds := currentNow.Unix() - preApy.TransactionTime.Unix()                            //上一次采集和当前采集相差秒
		perSecondApy := ((preExchangeRateF - exchangeRateF) / exchangeRateF) / float64(differenceInSeconds) //每秒apy
		yearApy = perSecondApy * 60 * 60 * 24 * 365
	}
	sql := "insert into vault_apy(vault_id,exchange_rate,apy,transaction_time) value(?,?,?,?)"
	result, err := con.Exec(sql, vaultId, exchangeRate, yearApy, time.Now())
	if err != nil {
		log.Printf("vault_apy新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apy新增id：=%v", lastInsertID)
	defer con.Close()
}

func QueryRowIsValue(err error) bool {
	switch {
	case err == sql.ErrNoRows:
		// 无数据场景：正常业务逻辑（如返回空、创建默认值等）
		log.Println("未查询到数据")
		return false
	case err != nil:
		// 其他错误：如SQL语法错误、字段类型不匹配、连接异常等
		log.Printf("查询失败：%v\n", err)
		return false
	default:
		// 查询成功：处理数据
		return true
	}
}

type VaultApyModel struct {
	Id              int
	VaultId         string
	Apy             string
	ExchangeRate    string
	TransactionTime time.Time
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
	sql := "select vault_id,vault_name,asset_type,htoken_type,asset_decimals from vault "
	rs, err := con.Query(sql)
	if err != nil {
		log.Printf("QueryVaultAll 查询失败: %v", err)
		defer con.Close()
		return vms
	}
	for rs.Next() {
		var vm VaultModel
		errScan := rs.Scan(&vm.VaultId, &vm.VaultName, &vm.AssetType, &vm.HtokenType, &vm.AssetDecimals)
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

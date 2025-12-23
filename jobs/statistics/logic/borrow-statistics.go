package statistics

import (
	"context"
	"log"
	"strconv"
	"time"

	"haedal-earn-borrow-server/common"
	"haedal-earn-borrow-server/jobs/borrow/logic"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
)

func MarketTimeCollection() {
	markets := QueryBorrowAll()
	if len(markets) > 0 {
		transaction_time := time.Now()
		transaction_time = transaction_time.Add(-30 * time.Second) //当前时间30秒前
		for _, market := range markets {
			BorrowTimeCollection(market, transaction_time, 2)
			BorrowSupplyTimeCollection(market, transaction_time, 2)
		}
	}
}

// borrow 借/还净值定时收集
func BorrowTimeCollection(marketInfo MarketModel, transaction_time time.Time, failRetryCount int) {
	cli := sui.NewSuiClient(logic.SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(logic.SuiUserAddress))
	marketId, err := strconv.ParseUint(marketInfo.MarketId, 10, 64)
	if err != nil {
		log.Printf("BorrowTimeCollection marketId转换失败：%v\n", err)
		return
	}
	arguments, parameErr := HearnAndMarketIdParameter(cli, ctx, *tx, marketId)
	typeArguments := []transaction.TypeTag{}
	if parameErr != nil {
		log.Printf("BorrowTimeCollection  parameErr:%v", parameErr.Error())
		BorrowCollectionFailRetry(marketInfo, transaction_time, failRetryCount)
		return
	}
	moduleName := "market"
	funcName := "market_total_borrow_assets"
	moveCallReturn := logic.ExecuteDevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, typeArguments, arguments)
	if len(moveCallReturn) > 0 {
		borrowAssets := "-1"
		for _, returnValue := range moveCallReturn[0].ReturnValues {
			bcsBytes, _ := logic.AnyToBytes(returnValue.([]any)[0])
			deserializer := bcs.NewDeserializer(bcsBytes)
			val := deserializer.U128()
			borrowAssets = val.String()
		}
		if borrowAssets != "-1" {
			InsertBorrowAssetsRecord(marketInfo, transaction_time, borrowAssets)
		}
	} else {
		BorrowCollectionFailRetry(marketInfo, transaction_time, failRetryCount)
	}
}

// borrow 池存/取净值定时收集
func BorrowSupplyTimeCollection(marketInfo MarketModel, transaction_time time.Time, failRetryCount int) {
	cli := sui.NewSuiClient(logic.SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(logic.SuiUserAddress))
	marketId, err := strconv.ParseUint(marketInfo.MarketId, 10, 64)
	if err != nil {
		log.Printf("borrowSupplyCollection marketId转换失败：%v\n", err)
		return
	}
	arguments, parameErr := HearnAndMarketIdParameter(cli, ctx, *tx, marketId)
	typeArguments := []transaction.TypeTag{}
	if parameErr != nil {
		log.Printf("EarnTvlTimedCollection  parameErr:%v", parameErr.Error())
		BorrowSupplyCollectionFailRetry(marketInfo, transaction_time, failRetryCount)
		return
	}
	moduleName := "market"
	funcName := "market_total_supply_assets"
	moveCallReturn := logic.ExecuteDevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, typeArguments, arguments)
	if len(moveCallReturn) > 0 {
		supplyAssets := "-1"
		for _, returnValue := range moveCallReturn[0].ReturnValues {
			bcsBytes, _ := logic.AnyToBytes(returnValue.([]any)[0])
			deserializer := bcs.NewDeserializer(bcsBytes)
			val := deserializer.U128()
			supplyAssets = val.String()
		}
		if supplyAssets != "-1" {
			InsertBorrowSupplyAssetsRecord(marketInfo, transaction_time, supplyAssets)
		}
	} else {
		BorrowSupplyCollectionFailRetry(marketInfo, transaction_time, failRetryCount)
	}
}

func InsertBorrowSupplyAssetsRecord(marketInfo MarketModel, transaction_time time.Time, supplyAssets string) {
	con := common.GetDbConnection()
	transaction_time_unix := transaction_time.UnixMilli() // 毫秒
	sql := "insert into borrow_supply_assets_record(market_id,total_asset,collateral_token_type,base_token_decimals,loan_token_type,quote_token_decimals,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, marketInfo.MarketId, supplyAssets, marketInfo.CollateralTokenType, marketInfo.CollateralCoinDecimals, marketInfo.LoanTokenType, marketInfo.LoanCoinDecimals, transaction_time_unix, transaction_time)
	if err != nil {
		log.Printf("borrow_supply_assets_record 新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_assets_record新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertBorrowAssetsRecord(marketInfo MarketModel, transaction_time time.Time, supplyAssets string) {
	con := common.GetDbConnection()
	transaction_time_unix := transaction_time.UnixMilli() // 毫秒
	sql := "insert into borrow_assets_record(market_id,total_asset,collateral_token_type,base_token_decimals,loan_token_type,quote_token_decimals,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, marketInfo.MarketId, supplyAssets, marketInfo.CollateralTokenType, marketInfo.CollateralCoinDecimals, marketInfo.LoanTokenType, marketInfo.LoanCoinDecimals, transaction_time_unix, transaction_time)
	if err != nil {
		log.Printf("borrow_assets_record 新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_assets_record新增id：=%v", lastInsertID)
	defer con.Close()
}

func BorrowSupplyCollectionFailRetry(marketInfo MarketModel, transaction_time time.Time, failRetryCount int) {
	if failRetryCount > 0 {
		failRetryCount = failRetryCount - 1
		BorrowSupplyTimeCollection(marketInfo, transaction_time, failRetryCount)
	}
}

func BorrowCollectionFailRetry(marketInfo MarketModel, transaction_time time.Time, failRetryCount int) {
	if failRetryCount > 0 {
		failRetryCount = failRetryCount - 1
		BorrowTimeCollection(marketInfo, transaction_time, failRetryCount)
	}
}

func HearnAndMarketIdParameter(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, marketId uint64) ([]transaction.Argument, error) {
	hearnSharedObject, err := logic.GetSharedObjectRef(ctx, cli, logic.HEarnObjectId, true)
	if err != nil {
		log.Printf("hearnSharedObject fail:%v", err.Error())
		return nil, err
	}
	arguments := []transaction.Argument{
		// 第一个参数：&Hearn 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: hearnSharedObject,
				},
			},
		),
		// 第二个参数：market_id
		tx.Pure(marketId),
	}
	return arguments, err
}

func QueryBorrowAll() []MarketModel {
	var markets []MarketModel
	sql := "SELECT id,market_id,market_title,collateral_token_type,loan_token_type,base_token_decimals,quote_token_decimals from borrow"
	con := common.GetDbConnection()
	rs, err := con.Query(sql)
	if err != nil {
		log.Printf("QueryBorrowAll 查询失败: %v", err)
		defer con.Close()
		return markets
	}
	for rs.Next() {
		var m MarketModel
		errScan := rs.Scan(&m.Id, &m.MarketId, &m.MarketTitle, &m.CollateralTokenType, &m.LoanTokenType, &m.CollateralCoinDecimals, &m.LoanCoinDecimals)
		if errScan != nil {
			log.Printf("QueryBorrowAll scan失败: %v", err)
			defer con.Close()
			return markets
		}
		markets = append(markets, m)
	}
	defer con.Close()
	return markets
}

type MarketModel struct {
	Id                     int
	MarketId               string
	MarketTitle            *string
	CollateralTokenType    string
	LoanTokenType          string
	CollateralCoinDecimals int8 // 抵押币种精度
	LoanCoinDecimals       int8 // 贷款币种精度
}

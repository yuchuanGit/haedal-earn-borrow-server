package logic

import (
	"context"
	"fmt"
	"haedal-earn-borrow-server/common"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/utils"
)

const (
	// PackageId = "0x9b9c9550a28834aebbc7a08e05bcd3e265fb99de559a61917ea1ca7571519f79"
	// PackageId = "0xa1de4394e5ce27aff798fc62297ea44d3c8a5932e6997898bb3fdedf44c7f2b1"
	// PackageId = "0x2edaecbb7006874ca2325cb64c5e270c75ddaa3ff6e538cef01f1776a44849ef" //11-18
	// PackageId = "0xb12af66b0efad99e73caa6a45abd899617514edaebbbcb0da323adee44d19960" // 11-20 10:00
	// PackageId      = "0x22e06514fa36ac6fa71d52e3af0781c6a90fd9e8819b3d03a175c8539bce7e27" // 11-20 16:15
	PackageId = "0x2174ea522ea7eec389d64cf9b5d4471fa4015b2c740030c1a34caffe2f21f88c"
	// PackageIdDev   = "0xea26bb0b34071bace5485272802d0b3dc158aca7ca729630d94130a56c092b46"
	// HEarnObjectId  = "0x280ede46d7c4f053a0b5a87c49857499b7a89f38720ad63b1650e0f1d85ef0f4"
	PackageIdDev   = "0x2174ea522ea7eec389d64cf9b5d4471fa4015b2c740030c1a34caffe2f21f88c"
	HEarnObjectId  = "0xbc5da89ed860e45d58a724921175224338f452e0896a5565d0564e2fb1b1ed85"
	SuiUserAddress = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
	SuiEnv         = "https://sui-testnet-endpoint.blockvision.org"
)

const (
	CreateMarketEvent     = "::events::CreateMarketEvent"
	SupplyEvent           = "::events::SupplyEvent"
	SupplyCollateralEvent = "::events::SupplyCollateralEvent"
	BorrowEvent           = "::events::BorrowEvent"
	RepayEvent            = "::events::RepayEvent"
	AccrueInterestEvent   = "::events::AccrueInterestEvent"
)

func RpcApiRequest() {
	cli := sui.NewSuiClient("https://sui-testnet-endpoint.blockvision.org")
	ctx := context.Background()
	resp, err := cli.SuiXQueryTransactionBlocks(ctx, models.SuiXQueryTransactionBlocksRequest{
		SuiTransactionBlockResponseQuery: models.SuiTransactionBlockResponseQuery{
			TransactionFilter: models.TransactionFilter{
				"MoveFunction": map[string]interface{}{
					"package": PackageId,
					"module":  "market_entry",
				},
			},
			Options: models.SuiTransactionBlockOptions{
				ShowInput:   true,
				ShowEffects: true,
				ShowEvents:  true,
			},
		},
	})
	if err != nil {
		fmt.Printf("RpcApiRequest err:%v\n", err)
	}
	for i := len(resp.Data) - 1; i >= 0; i-- {
		data := resp.Data[i]
		digest := data.Digest
		transactionTime := data.TimestampMs
		transactions := data.Transaction.Data.Transaction.Transactions
		for _, event := range data.Events {
			eventType := strings.Replace(event.Type, PackageId, "", 1)
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
			case AccrueInterestEvent: //计利息
				InsertRateDetali(event.ParsedJson, transactions, digest, transactionTime)
			}
		}
	}

	// for _, data := range resp.Data {
	// 	digest := data.Digest
	// 	transactionTime := data.TimestampMs
	// 	for _, event := range data.Events {
	// 		eventType := strings.Replace(event.Type, PackageId, "", 1)
	// 		switch eventType {
	// 		case CreateMarketEvent:
	// 			InsertBorrow(event.ParsedJson, digest, transactionTime)
	// 		case SupplyEvent: //存操作
	// 			InsertBorrowDetali(event.ParsedJson, digest, transactionTime)
	// 		case AccrueInterestEvent: //计利息

	// 		}
	// 	}
	// }
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
			return
		}
		if queryRs.Next() {
			fmt.Printf("AccrueInterestEvent digest exist :%v\n", digest)
			defer queryRs.Close()
			return
		}
		sql := "insert into rate_detail(rate_type,market_id,collateral_token_type,loan_token_type,borrow_rate,fee_shares,interest,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
		result, err := con.Exec(sql, funcName, market_id, collateralType, loanlType, borrowRate, feeShares, interest, digest, transactionTimeUnix, transactionTime)
		if err != nil {
			log.Printf("rate_detail新增失败: %v", err)
			return
		}
		lastInsertID, _ := result.LastInsertId()
		log.Printf("rate_detail新增id：=%v", lastInsertID)
		defer con.Close()
	}

}

func InsertBorrowRepayDetali(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	market_id := parsedJson["market_id"].(string)
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_repay_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		return
	}
	if queryRs.Next() {
		fmt.Printf("RepayEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		return
	}

	sql := "insert into borrow_repay_detail(market_id,caller_address,on_behalf_address,assets,shares,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, assets, shares, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_repay_detail新增失败: %v", err)
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
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		return
	}
	if queryRs.Next() {
		fmt.Printf("BorrowEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		return
	}

	sql := "insert into borrow_detail(market_id,caller_address,on_behalf_address,receiver_address,assets,shares,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, market_id, caller_address, on_behalf_address, receiver_address, assets, shares, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_detail新增失败: %v", err)
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_detail新增id：=%v", lastInsertID)

	borrowRs, borrowErr := con.Query("select * from borrow where market_id=?", market_id)
	if borrowErr != nil {
		log.Printf("borrow查询market_id失败: %v", borrowErr)
		return
	}
	if borrowRs.Next() {
		log.Printf("borrow-market_id查询1")
		upRs, upErr := con.Exec("update borrow set total_loan_amount=total_loan_amount+? where market_id=?", assets, market_id)
		if upErr != nil {
			log.Printf("borrow 更新失败: %v", upErr)
			return
		}
		upRow, _ := upRs.RowsAffected()
		log.Printf("borrow 更新条数: %v", upRow)
	}
	defer con.Close() // 程序退出时关闭数据库连接
}

func InsertBorrowSupplyDetaliCollateral(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	market_id := parsedJson["market_id"].(string)
	assets := parsedJson["assets"].(string)
	con := common.GetDbConnection()
	convRs, convErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(convRs)
	queryRs, queryErr := con.Query("select * from borrow_supply_detail where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		return
	}
	if queryRs.Next() {
		fmt.Printf("SupplyCollateralEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		return
	}

	sql := "insert into borrow_supply_detail(supply_type,market_id,caller_address,on_behalf_address,assets,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, 2, market_id, caller_address, on_behalf_address, assets, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("borrow_supply_detail_collateral新增失败: %v", err)
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_detail_collateral新增id：=%v", lastInsertID)

	borrowRs, borrowErr := con.Query("select * from borrow where market_id=?", market_id)
	if borrowErr != nil {
		log.Printf("borrow查询market_id失败: %v", borrowErr)
		return
	}
	if borrowRs.Next() {
		upRs, upErr := con.Exec("update borrow set total_supply_collateral_amount=total_supply_collateral_amount+? where market_id=?", assets, market_id)
		if upErr != nil {
			log.Printf("borrow 更新失败: %v", upErr)
			return
		}
		upRow, _ := upRs.RowsAffected()
		log.Printf("borrow 更新条数: %v", upRow)
	}
	defer con.Close() // 程序退出时关闭数据库连接
}
func InsertBorrowSupplyDetali(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	caller_address := parsedJson["caller"].(string)
	on_behalf_address := parsedJson["on_behalf"].(string)
	market_id := parsedJson["market_id"].(string)
	assets := parsedJson["assets"].(string)
	shares := parsedJson["shares"].(string)
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
		return
	}
	if queryRs.Next() {
		fmt.Printf("SupplyEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集
		return
	}

	sql := "insert into borrow_supply_detail(supply_type,market_id,caller_address,on_behalf_address,assets,shares,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, 1, market_id, caller_address, on_behalf_address, assets, shares, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("新增失败: %v", err)
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("borrow_supply_detail新增id：=%v", lastInsertID)

	borrowRs, borrowErr := con.Query("select * from borrow where market_id=?", market_id)
	if borrowErr != nil {
		log.Printf("borrow查询market_id失败: %v", borrowErr)
		return
	}
	if borrowRs.Next() {
		upRs, upErr := con.Exec("update borrow set total_supply_amount=total_supply_amount+? where market_id=?", assets, market_id)
		if upErr != nil {
			log.Printf("borrow 更新失败: %v", upErr)
			return
		}
		upRow, _ := upRs.RowsAffected()
		log.Printf("borrow 更新条数: %v", upRow)
	}
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
	utils.PrettyPrint(titleBcsByte)
	deserializer := bcs.NewDeserializer(titleBcsByte)
	title := deserializer.ReadString()
	// title := ""
	baseTokenDecimals := parsedJson["base_token_decimals"].(string)
	quoteTokenDecimals := parsedJson["quote_token_decimals"].(string)

	log.Printf("title=%v", title)
	log.Printf("baseTokenDecimals=%v", baseTokenDecimals)
	log.Printf("quoteTokenDecimals=%v", quoteTokenDecimals)

	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from borrow where digest=?", digest)
	if queryErr != nil {
		log.Printf("borrow查询 digest失败: %v", queryErr)
		return
	}

	if queryRs.Next() {
		fmt.Printf("CreateMarketEvent digest exist :%v\n", digest)
		defer queryRs.Close() // 务必关闭结果集，避免资源泄漏
		return
	}

	sql := "insert into borrow(digest,market_title,collateral_token_type,fee,lltv,ltv,loan_token_type,market_id,oracle_id,base_token_decimals,quote_token_decimals,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, digest, title, collateralType, fee, lltv, ltv, loanType, marketId, oracleId, baseTokenDecimals, quoteTokenDecimals, transactionTime)
	if err != nil {
		log.Printf("新增失败: %v", err)
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Println("新增id：=", lastInsertID)
	defer con.Close() // 程序退出时关闭数据库连接
}

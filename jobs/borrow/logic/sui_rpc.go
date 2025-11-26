package logic

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"haedal-earn-borrow-server/common"

	"log"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/mystenbcs"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
)

func UpdateBorrowInfo() {
	con := common.GetDbConnection()
	sql := "SELECT * from borrow where scheduled_execution=0"
	rs, err := con.Query(sql)
	if err != nil {
		// fmt.Errorf(err.Error()
		return
	}
	for rs.Next() {

	}
	defer con.Close()
}

func UpdateMarketRate() {
	con := common.GetDbConnection()
	sql := "SELECT market_id from borrow where scheduled_execution=0 limit 2"
	rs, err := con.Query(sql)
	if err != nil {
		fmt.Printf("UpdateMarketRate 查询borrow失败：%v\n", err.Error())
	}
	var marketIds []uint64
	for rs.Next() {
		var marketId uint64
		rs.Scan(&marketId)
		marketIds = append(marketIds, marketId)
	}
	if len(marketIds) == 1 {
		fmt.Printf("最后一条：%v\n", marketIds[0])
		UpdateBorrowRate(marketIds[0], con)
		upSql := "update borrow set scheduled_execution=0"
		_, upErr := con.Exec(upSql)
		if upErr != nil {
			fmt.Printf("UpdateMarketRate borrow scheduled_execution=0失败：%v\n", upErr.Error())
		}
	} else if len(marketIds) > 1 {
		UpdateBorrowRate(marketIds[0], con)
	}
	defer con.Close()
	// marketInfo := GetMarketInfo()
	// if len(marketInfo) > 0 {
	// 	supplyRate := marketInfo[2]
	// 	borrowRate := marketInfo[3]
	// 	log.Printf("supplyRate=%v", supplyRate)
	// 	log.Printf("borrowRate=%v", borrowRate)
	// }
}

func UpdateBorrowRate(marketId uint64, con *sql.DB) {
	marketInfos := GetMarketInfo(marketId)
	var baseUnit float64 = 1e16
	var maxUtilization float64 = 1
	if len(marketInfos) > 0 {
		marketInfo := marketInfos[0]

		// supplyRate, _ := strconv.ParseFloat(marketInfo[2], 64)        //存利率
		// borrowRate, _ := strconv.ParseFloat(marketInfo[3], 64)        //借利率
		// totalSupplyAssets, _ := strconv.ParseFloat(marketInfo[8], 64) //总存入数量
		// totalBorrowAssets, _ := strconv.ParseFloat(marketInfo[9], 64) //总借出数量
		supplyRate := float64(marketInfo.SupplyRate)                       //存利率
		borrowRate := float64(marketInfo.BorrowRate)                       //借利率
		totalSupplyAssets := float64(marketInfo.TotalSupplyAssets)         //总存入数量
		totalCollateralAssets := float64(marketInfo.TotalCollateralAssets) //总抵押
		totalBorrowAssets := float64(marketInfo.TotalBorrowAssets)         //总借出数量
		liquidityProportion := 0.00
		if totalBorrowAssets > 0 {
			liquidityProportion = (totalBorrowAssets / (totalSupplyAssets * maxUtilization)) * 100
		}
		liquidity := totalSupplyAssets - totalBorrowAssets
		supplyRate = supplyRate / baseUnit
		borrowRate = borrowRate / baseUnit
		supplyRateStr := fmt.Sprintf("%.2f", supplyRate) + "%"
		borrowRateStr := fmt.Sprintf("%.2f", borrowRate) + "%"
		if supplyRate < 0.01 {
			supplyRateStr = "<0.01%"
		}
		if borrowRate < 0.01 {
			borrowRateStr = "<0.01%"
		}
		liquidityProportionStr := fmt.Sprintf("%.16f", liquidityProportion)
		title := "HEARN-USDC-SUI"
		upSql := "update borrow set total_supply_amount=?,total_supply_collateral_amount=?,total_loan_amount=?,supply_rate=?,borrow_rate=?,liquidity=?,liquidity_proportion=?,market_title=?,scheduled_execution=1 where market_id=?"
		_, upErr := con.Exec(upSql, totalSupplyAssets, totalCollateralAssets, totalBorrowAssets, supplyRateStr, borrowRateStr, liquidity, liquidityProportionStr, title, marketId)
		if upErr != nil {
			fmt.Printf("UpdateMarketRate update rate失败：%v\n", upErr.Error())
		}
	}
}

func GetMarketInfo(marketId uint64) []MarketInfo {
	var initVal []MarketInfo
	cli := sui.NewSuiClient(SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(SuiUserAddress))
	arguments, parameErr := GetMarketInfoParameter(cli, ctx, *tx, marketId)
	if parameErr != nil {
		return initVal
	}
	moduleName := "market"
	funcName := "market_info"
	return DevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, arguments)
}

func DevInspectTransactionBlock(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, moduleName string, funcName string, arguments []transaction.Argument) []MarketInfo {
	var initVal []MarketInfo
	tx.MoveCall(
		models.SuiAddress(PackageIdDev),
		moduleName,
		funcName,
		[]transaction.TypeTag{},
		arguments,
	)

	bcsEncodedMsg, err := tx.Data.V1.Kind.Marshal()
	txBytes := mystenbcs.ToBase64(bcsEncodedMsg)
	if err != nil {
		fmt.Printf("tx.Data.V1.Kind.Marshal查询失败：%v\n", err)
		return initVal
	}

	devRs, devErr := cli.SuiDevInspectTransactionBlock(ctx, models.SuiDevInspectTransactionBlockRequest{
		Sender:  SuiUserAddress,
		TxBytes: txBytes,
	})
	if devErr != nil {
		fmt.Printf("SuiDevInspectTransactionBlock查询失败：%v\n", devErr.Error())
		return initVal
	}
	if devRs.Effects.Status.Status == "failure" {
		fmt.Println("SuiDevInspectTransactionBlock Status 失败:", devRs.Effects.Status.Error)
		return initVal
	}
	resultsMarshalled, err2 := devRs.Results.MarshalJSON()
	if err2 != nil {
		fmt.Println("SuiDevInspectTransactionBlock MarshalJSON 失败:", err2.Error())
		return initVal
	}

	type moveCallResult struct {
		ReturnValues []interface{}
	}
	var moveCallReturn []moveCallResult
	err3 := json.Unmarshal(resultsMarshalled, &moveCallReturn)
	if err3 != nil {
		fmt.Println("moveCallReturn convert json fail ", err3.Error())
		return initVal
	}
	var resultData []MarketInfo
	for _, returnValue := range moveCallReturn[0].ReturnValues {
		var market MarketInfo
		bcsBytes, _ := anyToBytes(returnValue.([]any)[0])
		deserializer := bcs.NewDeserializer(bcsBytes)
		if err := market.UnmarshalBCS(deserializer); err != nil {
			panic(fmt.Sprintf("解析 MarketInfo 失败：%v", err))
		}
		resultData = append(resultData, market)
		// bcsBytes, _ := anyToBytes(returnValue.([]any)[0])
		// dataType := returnValue.([]any)[1]
		// val := bcsTypeDistinctionResult(dataType, bcsBytes)
		// resultData = append(resultData, val)
	}
	return resultData
}

func (m *MarketInfo) UnmarshalBCS(d *bcs.Deserializer) error {
	// 按 Move 字段顺序解析
	// marketId := d.U64() // 解析 u128
	m.MarketId = d.U64()
	m.SupplyCoinType = d.ReadString()
	m.CollateralCoinType = d.ReadString()
	m.Ltv = d.U64()
	m.Lltv = d.U64()
	m.LiquidationThreshold = d.U64()
	m.TotalSupplyAssets = d.U64()
	m.TotalBorrowAssets = d.U64()
	m.TotalCollateralAssets = d.U64()
	m.Fee = d.U64()
	m.FlashloanFee = d.U64()
	m.SupplyRate = d.U64()
	m.BorrowRate = d.U64()
	m.UtilizationRate = d.U64()
	m.MarketPaused = d.Bool()
	m.GlobalPaused = d.Bool()
	// titleByte := d.ReadBytes()

	// deserializer := bcs.NewDeserializer(titleByte)
	// log.Printf("title=%b", )

	m.Title = d.ReadString()
	return nil
}

type MarketInfo struct {
	MarketId              uint64 `bcs:"market_id"`
	SupplyCoinType        string `bcs:"supply_coin_type"`
	CollateralCoinType    string `bcs:"collateral_coin_type"`
	Ltv                   uint64 `bcs:"ltv"`
	Lltv                  uint64 `bcs:"lltv"`                    // Liquidation Loan-to-Value in WAD
	LiquidationThreshold  uint64 `bcs:"liquidation_threshold"`   // Liquidation threshold (same as LLTV)
	TotalSupplyAssets     uint64 `bcs:"total_supply_assets"`     // Total supplied assets
	TotalBorrowAssets     uint64 `bcs:"total_borrow_assets"`     // Total borrowed assets
	TotalCollateralAssets uint64 `bcs:"total_collateral_assets"` // Total collateral assets
	Fee                   uint64 `bcs:"fee"`                     // Protocol fee in WAD
	FlashloanFee          uint64 `bcs:"flashloan_fee"`           // Flash loan fee in WAD
	SupplyRate            uint64 `bcs:"supply_rate"`             // Current supply rate (WAD precision)
	BorrowRate            uint64 `bcs:"borrow_rate"`             // Current borrow rate (WAD precision)
	UtilizationRate       uint64 `bcs:"utilization_rate"`        // Market utilization rate (WAD precision, total_borrow / total_supply)
	MarketPaused          bool   `bcs:"market_paused"`           // Market-level pause flag
	GlobalPaused          bool   `bcs:"global_paused"`           // Global pause flag
	Title                 string `bcs:"title"`                   // Human readable market title
}

// 实现 bcs.Marshaler 接口（可选，默认通过反射）

func GetMarketInfoParameter(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, marketId uint64) ([]transaction.Argument, error) {
	hearnSharedObject, err := GetSharedObjectRef(ctx, cli, HEarnObjectId, true)
	clockSharedObject, err2 := GetSharedObjectRef(ctx, cli, "0x6", true)
	// marketID := uint64(2)
	// marketIDs := []uint64{1, 2}
	if err != nil {
		log.Printf("hearnSharedObject fail:%v", err.Error())
		return nil, err
	}
	if err2 != nil {
		log.Printf("clockSharedObject fail:%v", err2.Error())
		return nil, err2
	}
	arguments := []transaction.Argument{
		// 第一个参数：&Hearn 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					// 非共享对象用 ImmOrOwnedObject
					// ImmOrOwnedObject: &transaction.SuiObjectRef{ObjectId: *objectIdBytes, Version: ver, Digest: *digestBytes},
					SharedObject: hearnSharedObject,
				},
			},
		),
		// 第二个参数：market_id
		tx.Pure(marketId),
		// 第三个参数：&Clock 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: clockSharedObject,
				},
			},
		),
	}

	return arguments, err
}

func bcsTypeDistinctionResult(dataType interface{}, bcsBytes []byte) string {
	deserializer := bcs.NewDeserializer(bcsBytes)
	if dataType == "0x1::type_name::TypeName" {
		return deserializer.ReadString()
	} else if dataType == "u64" {
		val := deserializer.U64()
		str := fmt.Sprintf("%d", val)
		return str
	} else if dataType == "u128" {
		val := deserializer.U128()
		return val.String()
	} else if dataType == "bool" {
		val := deserializer.Bool()
		str := fmt.Sprintf("%t", val)
		return str
	} else if dataType == "vector<u8>" {
		// log.Printf("returnValue:%v\n", deserializer.ReadString())
		return deserializer.ReadString()
	}
	return ""
}

func anyToBytes(anyData any) ([]byte, error) {
	switch val := anyData.(type) {
	case []any:
		// 处理 []any（JSON 数组）
		byteSlice := make([]byte, len(val))
		for i, elem := range val {
			num, ok := elem.(float64)
			if !ok || num < 0 || num > 255 {
				return nil, fmt.Errorf("[]any 元素 %d 非法：%v（类型：%T）", i, elem, elem)
			}
			byteSlice[i] = byte(num)
		}
		return byteSlice, nil

	case string:
		// 处理 string
		return []byte(val), nil

	case uint64:
		// 处理 uint64（大端序）
		byteSlice := make([]byte, 8)
		binary.BigEndian.PutUint64(byteSlice, val)
		return byteSlice, nil

	default:
		return nil, fmt.Errorf("不支持的类型：%T", anyData)
	}
}

func GetSharedObjectRef(ctx context.Context, client sui.ISuiAPI, objectId string, mutable bool) (*transaction.SharedObjectRef, error) {
	rsp, err := client.SuiGetObject(ctx, models.SuiGetObjectRequest{ObjectId: objectId, Options: models.SuiObjectDataOptions{
		ShowBcs:                 true,
		ShowOwner:               true,
		ShowPreviousTransaction: true,
		ShowDisplay:             true,
		ShowType:                true,
		ShowContent:             true,
		ShowStorageRebate:       true,
	}})
	if err != nil {
		return nil, err
	}
	if owner, ok := rsp.Data.Owner.(map[string]any); ok {
		if value, exists := owner["Shared"]; exists {
			rv := value.(map[string]interface{})["initial_shared_version"].(float64)
			// rv := value.(map[string]interface{})["initial_shared_version"].(string)
			obj, _ := transaction.ConvertSuiAddressStringToBytes(models.SuiAddress(objectId))
			// versionUint64, _ := strconv.ParseUint(rv, 10, 64)
			sharedObj := transaction.SharedObjectRef{
				ObjectId:             *obj,
				InitialSharedVersion: uint64(rv),
				// InitialSharedVersion: versionUint64,
				Mutable: mutable,
			}
			return &sharedObj, nil
		}
	}
	return nil, fmt.Errorf("object is not a shared object")
}

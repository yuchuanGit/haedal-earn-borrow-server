package logic

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"haedal-earn-borrow-server/common"
	"math"
	"math/big"
	"strconv"
	"strings"

	"log"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/mystenbcs"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
)

func InsertClearingUser() {
	var loanUsers []LoanUserInfo
	con := common.GetDbConnection()
	sql := "SELECT loanUser.market_id,loanUser.caller_address, " +
		"ccc.feed_id as collateralFeedId,ccc.feed_object_id as collateralFeedObjectId,ccl.feed_id as loanFeedId,ccl.feed_object_id as loanFeedObjectId " +
		"from (SELECT market_id,caller_address,max(collateral_token_type) collateral_token_type, max(loan_token_type) loan_token_type from borrow_detail GROUP BY caller_address,market_id) loanUser " +
		"left join coin_config ccc on ccc.coin_type=loanUser.collateral_token_type " +
		"left join coin_config ccl on ccl.coin_type=loanUser.loan_token_type "
	rs, err := con.Query(sql)
	if err != nil {
		fmt.Printf(" 查询borrow_detail失败：%v\n", err.Error())
		defer con.Close()
		return
	}
	feedIds := make(map[string]string)
	for rs.Next() {
		var userInfo LoanUserInfo
		rs.Scan(&userInfo.MarketId, &userInfo.UserAddress, &userInfo.CollateralFeedId, &userInfo.CollateralFeedObjectId, &userInfo.LoanFeedId, &userInfo.LoanFeedObjectId)
		loanUsers = append(loanUsers, userInfo)
		feedIds[userInfo.CollateralFeedId] = userInfo.CollateralFeedId
		feedIds[userInfo.LoanFeedId] = userInfo.LoanFeedId
	}
	log.Printf("loanUsers-length=：%v", len(loanUsers))
	if len(loanUsers) > 0 {
		cli := sui.NewSuiClient(SuiEnv)
		ctx := context.Background()
		tx := transaction.NewTransaction()
		tx.SetSuiClient(cli.(*sui.Client))
		tx.SetSender(models.SuiAddress(SuiUserAddress))
		var clearingUsers []UserPositionInfo
		// cli.SuiDevInspectTransactionBlock()
		// coinPrice := common.PythPrice(feedIds)
		// utils.PrettyPrint(coinPrice)
		// clien
		for _, loanUser := range loanUsers {
			arguments, parameErr := userPositionInfoParameter(cli, ctx, *tx, loanUser)
			if parameErr != nil {
				log.Printf("userPositionInfo arguments fail:%v", parameErr.Error())
				return
			}
			moduleName := "market"
			funcName := "user_position_info"
			typeArguments := []transaction.TypeTag{}
			// log.Printf("UserAddress=：%v", loanUser.UserAddress)
			// log.Printf("CollateralFeedId=：%v", loanUser.CollateralFeedId)
			// log.Printf("LoanFeedId=：%v", loanUser.LoanFeedId)

			moveCallReturn := ExecuteDevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, typeArguments, arguments)
			if len(moveCallReturn) > 0 {
				for _, returnValue := range moveCallReturn[0].ReturnValues {
					bcsBytes, _ := anyToBytes(returnValue.([]any)[0])
					deserializer := bcs.NewDeserializer(bcsBytes)
					var userPosition UserPositionInfo
					if err := userPosition.UnmarshalBCS(deserializer); err != nil {
						panic(fmt.Sprintf("解析 UserPositionInfo 失败：%v", err))
					} else {
						log.Printf("UserAddress=：%v", loanUser.UserAddress)
						log.Printf("loanUser-MarketId=：%v", loanUser.MarketId)
						log.Printf("userPosition-MarketId=：%v", userPosition.MarketId)
						// log.Printf("CollateralFeedId=：%v", loanUser.CollateralFeedId)
						// log.Printf("LoanFeedId=：%v", loanUser.LoanFeedId)
						powResult := math.Pow(10, 18)
						HealthFactorF64, _ := strconv.ParseFloat(userPosition.HealthFactor.String(), 64)
						healthFactorRs := powResult / HealthFactorF64
						if healthFactorRs >= 0.95 {
							userPosition.UserAddress = loanUser.UserAddress
							clearingUsers = append(clearingUsers, userPosition)
						}
					}
				}
			} else {
				//todo 失败处理
			}
		}
		if len(clearingUsers) > 0 {
			clearSql := "TRUNCATE TABLE clearing_user"
			_, clearErr := con.Exec(clearSql)
			if clearErr != nil {
				fmt.Printf("clearing_user清理数据失败：%v\n", err.Error())
				defer con.Close()
				return
			}
			insertBase := "insert into clearing_user(user_address,market_id,supply_assets,supply_shares,collateral,borrow_assets,borrow_shares,health_factor,withdrawable_assets,max_borrowable,max_withdrawable_collateral) VALUES "
			// insertSql := insertBase
			var insertSql strings.Builder
			insertSql.WriteString(insertBase)
			var args []any
			for i := 0; i < len(clearingUsers); i++ {
				if i == len(clearingUsers)-1 {
					insertSql.WriteString("(?,?,?,?,?,?,?,?,?,?,?)")
				} else {
					insertSql.WriteString("(?,?,?,?,?,?,?,?,?,?,?),")
				}
				args = append(args, clearingUsers[i].UserAddress)
				args = append(args, clearingUsers[i].MarketId)
				args = append(args, clearingUsers[i].SupplyAssets)
				args = append(args, clearingUsers[i].SupplyShares)
				args = append(args, clearingUsers[i].Collateral)
				args = append(args, clearingUsers[i].BorrowAssets)
				args = append(args, clearingUsers[i].BorrowShares)
				args = append(args, clearingUsers[i].HealthFactor)
				args = append(args, clearingUsers[i].WithdrawableAssets)
				args = append(args, clearingUsers[i].MaxBorrowable)
				args = append(args, clearingUsers[i].MaxWithdrawableCollateral)
			}
			log.Printf("insertSql=：%v", insertSql.String())
			_, insertErr := con.Exec(insertSql.String(), args)
			if insertErr != nil {
				log.Printf("clearing_user 新增错误：%v", insertErr.Error())
				defer con.Close()
				return
			}
		} else {
			log.Println("clearingUsers 没有>=95%")
		}
	} else {
		log.Println("无借款用户...")
	}
	defer con.Close()
}

type LoanUserInfo struct {
	MarketId               uint64
	UserAddress            string
	CollateralFeedId       string
	CollateralFeedObjectId string
	LoanFeedId             string
	LoanFeedObjectId       string
}

type UserPositionInfo struct {
	MarketId                  uint64  `bcs:"market_id"`
	Collateral                big.Int `bcs:"collateral"`                  // User's collateral amount
	BorrowShares              big.Int `bcs:"borrow_shares"`               // User's borrow shares
	BorrowAssets              big.Int `bcs:"borrow_assets"`               // User's borrow assets (converted from shares)
	SupplyShares              big.Int `bcs:"supply_shares"`               // User's supply shares
	SupplyAssets              big.Int `bcs:"supply_assets"`               // User's supply assets (converted from shares)
	HealthFactor              big.Int `bcs:"health_factor"`               // Health factor (WAD precision, < 1e18 means liquidatable)
	WithdrawableAssets        big.Int `bcs:"withdrawable_assets"`         // Maximum withdrawable supply assets
	MaxBorrowable             big.Int `bcs:"max_borrowable"`              // Maximum borrowable amount
	MaxWithdrawableCollateral big.Int `bcs:"max_withdrawable_collateral"` // Maximum withdrawable collateral without breaching LLTV
	UserAddress               string
}

func (m *UserPositionInfo) UnmarshalBCS(d *bcs.Deserializer) error {
	var u UserPositionInfo
	u.MarketId = d.U64()
	u.Collateral = d.U128()
	u.BorrowShares = d.U128()
	u.BorrowAssets = d.U128()
	u.SupplyShares = d.U128()
	u.SupplyAssets = d.U128()
	u.HealthFactor = d.U128()
	u.WithdrawableAssets = d.U128()
	u.MaxBorrowable = d.U128()
	u.MaxWithdrawableCollateral = d.U128()
	return nil
}

func userPositionInfoParameter(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction,
	userInfo LoanUserInfo) ([]transaction.Argument, error) {
	hearnSharedObject, err := GetSharedObjectRef(ctx, cli, HEarnObjectId, true)
	oracleSharedObject, err4 := GetSharedObjectRef(ctx, cli, OracleObjectId, true)
	suppplyCollateralFeedSharedObject, err5 := GetSharedObjectRef(ctx, cli, userInfo.CollateralFeedObjectId, true)
	loanFeedSharedObject, err6 := GetSharedObjectRef(ctx, cli, userInfo.LoanFeedObjectId, true)
	clockSharedObject, err7 := GetSharedObjectRef(ctx, cli, "0x6", true)
	if err != nil {
		log.Printf("userPositionInfo hearnSharedObject parameter 1 fail:%v", err.Error())
		return nil, err
	}
	if err4 != nil {
		log.Printf("userPositionInfo oracleSharedObject parameter 4 fail:%v", err4.Error())
		return nil, err4
	}
	if err5 != nil {
		log.Printf("userPositionInfo suppplyCollateralFeedSharedObject parameter 5 fail:%v", err5.Error())
		return nil, err5
	}
	if err6 != nil {
		log.Printf("userPositionInfo loanFeedSharedObject parameter 6 fail:%v", err6.Error())
		return nil, err6
	}
	if err7 != nil {
		log.Printf("userPositionInfo clockSharedObject parameter 7 fail:%v", err7.Error())
		return nil, err7
	}
	arguments := []transaction.Argument{
		// 第一个参数：&Hear 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: hearnSharedObject,
				},
			},
		),
		// 第二个参数：market_id
		tx.Pure(userInfo.MarketId),
		// 第三个参数：owner
		tx.Pure(userInfo.UserAddress),
		// 第四个参数：&pyth_oracle::Oracle 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: oracleSharedObject,
				},
			},
		),
		// 第五个参数：&PriceInfoObject 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: suppplyCollateralFeedSharedObject,
				},
			},
		),
		// 第六个参数：&PriceInfoObject 对象
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					SharedObject: loanFeedSharedObject,
				},
			},
		),
		// 第七个参数：&Clock 对象
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

func UpdateMarketRate() {
	con := common.GetDbConnection()
	sql := "SELECT market_id from borrow where scheduled_execution=0"
	rs, err := con.Query(sql)
	if err != nil {
		fmt.Printf("UpdateMarketRate 查询borrow失败：%v\n", err.Error())
		defer con.Close()
		return
	}
	var marketIds []uint64
	for rs.Next() {
		var marketId uint64
		rs.Scan(&marketId)
		marketIds = append(marketIds, marketId)
	}
	lastIdx := len(marketIds) - 1
	for idx, marketId := range marketIds {
		UpdateBorrowRate(marketId, con)
		if idx == lastIdx {
			upSql := "update borrow set scheduled_execution=0"
			_, upErr := con.Exec(upSql)
			if upErr != nil {
				fmt.Printf("UpdateMarketRate borrow scheduled_execution=0失败：%v\n", upErr.Error())
			}
		}
	}
	// if len(marketIds) == 1 {
	// 	fmt.Printf("最后一条：%v\n", marketIds[0])
	// 	UpdateBorrowRate(marketIds[0], con)
	// 	upSql := "update borrow set scheduled_execution=0"
	// 	_, upErr := con.Exec(upSql)
	// 	if upErr != nil {
	// 		fmt.Printf("UpdateMarketRate borrow scheduled_execution=0失败：%v\n", upErr.Error())
	// 	}
	// } else if len(marketIds) > 1 {
	// 	UpdateBorrowRate(marketIds[0], con)
	// }
	defer con.Close()
}

func bigIntToFloat64(bi big.Int) (float64, bool) {
	bf := new(big.Float).SetInt(&bi)
	f64, exact := bf.Float64()
	return f64, exact == big.Exact // exact为true表示无精度丢失
}

func UpdateBorrowRate(marketId uint64, con *sql.DB) {
	marketInfos := GetMarketInfo(marketId)
	var baseUnit float64 = 1e16
	var maxUtilization float64 = 1
	if len(marketInfos) > 0 {
		marketInfo := marketInfos[0]
		supplyRate, _ := bigIntToFloat64(marketInfo.SupplyRate)                       //存利率
		borrowRate, _ := bigIntToFloat64(marketInfo.BorrowRate)                       //借利率
		totalSupplyAssets, _ := bigIntToFloat64(marketInfo.TotalSupplyAssets)         //总存入数量
		totalCollateralAssets, _ := bigIntToFloat64(marketInfo.TotalCollateralAssets) //总抵押
		totalBorrowAssets, _ := bigIntToFloat64(marketInfo.TotalBorrowAssets)         //总借出数量
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
		title := marketInfo.Title
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

type moveCallResult struct {
	ReturnValues []interface{}
}

func DevInspectTransactionBlock(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, moduleName string, funcName string, arguments []transaction.Argument) []MarketInfo {
	var initVal []MarketInfo
	tx.MoveCall(
		models.SuiAddress(PackageId),
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
	m.TotalSupplyAssets = d.U128()
	m.TotalBorrowAssets = d.U128()
	m.TotalCollateralAssets = d.U128()
	m.Fee = d.U128()
	m.FlashloanFee = d.U128()
	m.SupplyRate = d.U128()
	m.BorrowRate = d.U128()
	m.UtilizationRate = d.U128()
	m.MarketPaused = d.Bool()
	m.GlobalPaused = d.Bool()
	m.IsSupplyPause = d.Bool()
	m.IsWithdrawPause = d.Bool()
	m.IsSupplyCollateralPause = d.Bool()
	m.IsWithdrawCollateralPause = d.Bool()
	m.IsBorrowPause = d.Bool()
	m.IsRepayPause = d.Bool()
	m.Title = d.ReadString()
	return nil
}

type MarketInfo struct {
	MarketId                  uint64  `bcs:"market_id"`
	SupplyCoinType            string  `bcs:"supply_coin_type"`
	CollateralCoinType        string  `bcs:"collateral_coin_type"`
	Ltv                       uint64  `bcs:"ltv"`
	Lltv                      uint64  `bcs:"lltv"`                    // Liquidation Loan-to-Value in WAD
	LiquidationThreshold      uint64  `bcs:"liquidation_threshold"`   // Liquidation threshold (same as LLTV)
	TotalSupplyAssets         big.Int `bcs:"total_supply_assets"`     // Total supplied assets
	TotalBorrowAssets         big.Int `bcs:"total_borrow_assets"`     // Total borrowed assets
	TotalCollateralAssets     big.Int `bcs:"total_collateral_assets"` // Total collateral assets
	Fee                       big.Int `bcs:"fee"`                     // Protocol fee in WAD
	FlashloanFee              big.Int `bcs:"flashloan_fee"`           // Flash loan fee in WAD
	SupplyRate                big.Int `bcs:"supply_rate"`             // Current supply rate (WAD precision)
	BorrowRate                big.Int `bcs:"borrow_rate"`             // Current borrow rate (WAD precision)
	UtilizationRate           big.Int `bcs:"utilization_rate"`        // Market utilization rate (WAD precision, total_borrow / total_supply)
	MarketPaused              bool    `bcs:"market_paused"`           // Market-level pause flag
	GlobalPaused              bool    `bcs:"global_paused"`           // Global pause flag
	IsSupplyPause             bool    `bcs:"is_supply_pause"`
	IsWithdrawPause           bool    `bcs:"is_withdraw_pause"`
	IsSupplyCollateralPause   bool    `bcs:"is_supply_collateral_pause"`
	IsWithdrawCollateralPause bool    `bcs:"is_withdraw_collateral_pause"`
	IsBorrowPause             bool    `bcs:"is_borrow_pause"`
	IsRepayPause              bool    `bcs:"is_repay_pause"`
	Title                     string  `bcs:"title"` // Human readable market title
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

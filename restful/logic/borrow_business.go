package logic

import (
	"encoding/json"
	"fmt"
	"haedal-earn-borrow-server/common"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	SUICoinType      = "SUI"
	SUIFeefId        = "0x50c67b3fd225db8912a424dd4baed60ffdde625ed2feaaf283724f9608fea266"
	USDCCoinType     = "USDC"
	USDCFeefId       = "0x41f3625971ca2ed2263e78573fe5ce23e13d2558ed3f2e47ab0f84fb9e7ae722"
	DateFormatYMDHms = ""
	DateFormatYMDHm  = "2006-01-02 15:04"
)

type BorrowModel struct {
	Id                          int
	MarketId                    string
	MarketTitle                 *string
	MarketLog                   *string
	TotalSupplyAmount           string
	TotalSupplyCollateralAmount string
	TotalLoanAmount             string
	LoanTokenType               string
	CollateralTokenType         string
	Fee                         string
	Lltv                        string
	Ltv                         string
	Pair                        string
	OracleId                    string
	CollateralFeedId            string
	CollateralFeedObjectId      string
	LoanFeedId                  string
	LoanFeedObjectId            string
	CollateralCoinDecimals      int8   //抵押币种精度
	LoanCoinDecimals            int8   //贷款币种精度
	SupplyRate                  string //存利率
	BorrowRate                  string //借利率
	Liquidity                   string //流动性
	LiquidityProportion         string //流动性占比

}

type UserBorrowDetaliVo struct {
	// UserAddress                  string
	MarketSupplyCollateralAssets int64 //市场抵押数量
	MarketSupplyAssets           int64 //市场数量
	MarketSupplyShares           int64 //市场数量

}

type BorrowDetailVo struct {
	TotalSupplyAmount           string //borrow池存放总额
	TotalSupplyCollateralAmount string //borrow池存放抵押总额
	TotalLoanAmount             string //borrow贷款总额
	CollateralTokenType         string //抵押币种
	LoanTokenType               string //贷款币种
	OracleId                    string //预言机id
	Lltv                        string // 清算阈值
	Ltv                         string
	Pair                        string
	Title                       string
	VaultAddress                string //Earn存款池地址
	CollateralCoinDecimals      int8   //抵押币种精度
	LoanCoinDecimals            int8   //贷款币种精度
	CollateralFeedId            string
	CollateralFeedObjectId      string
	LoanFeedId                  string
	LoanFeedObjectId            string
	SupplyRate                  string //存利率
	BorrowRate                  string //借利率
	Liquidity                   string //流动性
	LiqPenalty                  string //清算者优惠比例
}

type BorrowLine struct {
	TransactionTime string
	DateUnit        string
	Amount          string
	TotalAmount     string
}

type UserTotalCollateraModel struct {
	TransactionTime string
	DateUnit        string
	Amount          string
	MarketId        string
	CoinType        string
	FeedId          string
}

type BorrowRateLine struct {
	TransactionTime string
	DateUnit        string
	InterestRate    string
}

type UserMarket struct {
	MarketId   string
	Assets     string
	MarketType int8 //1 存 2 借
}

type RateModel struct {
	TargetU  string            //模板最佳利用率
	CurrentU string            //当前利用率
	Rates    []RateModelDetail //利用率详情

}

type RateModelDetail struct {
	DateUnit   string
	SupplyRate string //存利率
	BorrowRate string //借利率
	CurrentU   string //当前利用率
}

func QueryBorrowDetail(marketId string) (BorrowDetailVo, error) {
	var vo BorrowDetailVo
	con := common.GetDbConnection()
	borrowSql := "select total_supply_amount,total_supply_collateral_amount,total_loan_amount,collateral_token_type,loan_token_type,oracle_id,lltv,ltv,market_title,supply_rate,borrow_rate,liquidity from borrow where market_id=?"
	err := con.QueryRow(borrowSql, marketId).Scan(
		&vo.TotalSupplyAmount,
		&vo.TotalSupplyCollateralAmount,
		&vo.TotalLoanAmount,
		&vo.CollateralTokenType,
		&vo.LoanTokenType,
		&vo.OracleId,
		&vo.Lltv,
		&vo.Ltv,
		&vo.Title,
		&vo.SupplyRate,
		&vo.BorrowRate,
		&vo.Liquidity,
	)

	vo.Lltv = LtvConvPercentage(vo.Lltv)
	vo.Ltv = LtvConvPercentage(vo.Ltv)
	collateralCoinType := GetStringDoubleSemicolonLast(vo.CollateralTokenType)
	loanCoinType := GetStringDoubleSemicolonLast(vo.LoanTokenType)
	vo.Pair = collateralCoinType + "/" + loanCoinType
	vo.CollateralCoinDecimals = GetCoinDecimal(collateralCoinType)
	vo.LoanCoinDecimals = GetCoinDecimal(loanCoinType)

	coinMap := GetCoinMap()
	collaCoin := coinMap[vo.CollateralTokenType]
	loanCoin := coinMap[vo.LoanTokenType]
	vo.CollateralFeedId = collaCoin.FeedId
	vo.CollateralFeedObjectId = collaCoin.FeedObjectId
	vo.LoanFeedId = loanCoin.FeedId
	vo.LoanFeedObjectId = loanCoin.FeedObjectId
	//todo
	vo.LiqPenalty = "3%"
	if err != nil {
		return vo, fmt.Errorf("查询borrow数据失败")
	}
	defer con.Close()
	return vo, nil
}

type RateDetail struct {
	DateUnit     string
	InterestRate string
	RateType     string
}

type SupplyBorrowGroup struct {
	DateUnit string
	Assets   string
}

func QueryBorrowDetailRateModel(marketId string) (RateModel, error) {
	var vo RateModel
	var ratesVo []RateModelDetail
	con := common.GetDbConnection()
	sql := "SELECT dateUnit,GROUP_CONCAT(interestRate) interestRate,GROUP_CONCAT(rate_type) rate_type from (" +
		"SELECT DATE_FORMAT(rd.transaction_time,'%Y-%m-%d') as dateUnit,rd.borrow_rate/10000000000000000 interestRate,rate_type " +
		"FROM rate_detail rd INNER JOIN (" +
		"SELECT DATE_FORMAT(transaction_time,'%Y-%m-%d') AS timeGroup, MAX(transaction_time) AS last_time FROM rate_detail " +
		"where market_id=? and rate_type in ('supply','borrow') GROUP BY timeGroup,rate_type " +
		") t ON DATE_FORMAT(transaction_time,'%Y-%m-%d') = t.timeGroup AND rd.transaction_time = t.last_time  ORDER BY rate_type DESC " +
		")a GROUP BY dateUnit"
	rs, err := con.Query(sql, marketId)

	supplySql := "SELECT IFNULL(sum(assets),0) supplyAssets,DATE_FORMAT(transaction_time,'%Y-%m-%d') timeGroup from borrow_supply_detail where market_id =? and supply_type=1  GROUP BY timeGroup"
	borrowSql := "SELECT IFNULL(sum(assets),0) borrowAssets,DATE_FORMAT(transaction_time,'%Y-%m-%d') timeGroup from borrow_detail  where market_id=? GROUP BY timeGroup"
	poolSql := "SELECT total_loan_amount/total_supply_amount currentU from borrow where market_id=?"
	supplyRs, supplyErr := con.Query(supplySql, marketId)
	borrowRs, borrowErr := con.Query(borrowSql, marketId)
	poolErr := con.QueryRow(poolSql, marketId).Scan(&vo.CurrentU)

	if err != nil {
		return vo, fmt.Errorf(err.Error())
	}
	if supplyErr != nil {
		return vo, fmt.Errorf(supplyErr.Error())
	}
	if borrowErr != nil {
		return vo, fmt.Errorf(borrowErr.Error())
	}
	if poolErr != nil {
		return vo, fmt.Errorf(poolErr.Error())
	}
	var rates []RateDetail
	for rs.Next() {
		var rd RateDetail
		rs.Scan(&rd.DateUnit, &rd.InterestRate, &rd.RateType)
		rates = append(rates, rd)
	}
	supplyTimeMap := make(map[string]string)
	borrowTimeMap := make(map[string]string)
	for supplyRs.Next() {
		var supply SupplyBorrowGroup
		supplyRs.Scan(&supply.Assets, &supply.DateUnit)
		supplyTimeMap[supply.DateUnit] = ScientificComputingConvStr(supply.Assets)
	}

	for borrowRs.Next() {
		var borrow SupplyBorrowGroup
		borrowRs.Scan(&borrow.Assets, &borrow.DateUnit)
		borrowTimeMap[borrow.DateUnit] = ScientificComputingConvStr(borrow.Assets)
	}
	for i := 0; i <= len(rates)-1; i++ {
		var rmd RateModelDetail
		rateTypes := strings.Split(rates[i].RateType, ",")
		interestRates := strings.Split(rates[i].InterestRate, ",")
		rmd.DateUnit = rates[i].DateUnit
		rmd.CurrentU = "0"
		if len(rateTypes) > 1 {
			rmd.SupplyRate = interestRates[0]
			rmd.BorrowRate = interestRates[1]
			s, _ := strconv.ParseFloat(rmd.SupplyRate, 64)
			b, _ := strconv.ParseFloat(rmd.BorrowRate, 64)
			u := b / s
			rmd.CurrentU = fmt.Sprintf("%.2f", u)
		} else if len(rateTypes) > 0 {
			if rateTypes[0] == "supply" {
				rmd.SupplyRate = interestRates[0]
				rmd.BorrowRate = "0"
			} else if rateTypes[0] == "borrow" {
				rmd.SupplyRate = "0"
				rmd.BorrowRate = interestRates[0]
			}
			daySupply := supplyTimeMap[rmd.DateUnit]
			dayBorrow := borrowTimeMap[rmd.DateUnit]
			// log.Printf("DateUnit: %v", rmd.DateUnit)
			if daySupply != "" {
				s, _ := strconv.ParseFloat(daySupply, 64)
				u := float64(50000000) / s
				rmd.CurrentU = fmt.Sprintf("%.2f", u)
			}
			if dayBorrow != "" {
				s, _ := strconv.ParseFloat(dayBorrow, 64)
				u := s / float64(26000000)
				rmd.CurrentU = fmt.Sprintf("%.2f", u)
			}
		}
		ratesVo = append(ratesVo, rmd)
		// log.Printf("supplyTimeMap: %v", supplyTimeMap[rmd.DateUnit])
		// log.Printf("borrowTimeMap: %v", borrowTimeMap[rmd.DateUnit])

	}
	vo.Rates = ratesVo
	vo.TargetU = "0.9"
	defer con.Close()
	return vo, nil
}

func GetCoinDecimal(typeVal string) int8 {
	switch typeVal {
	case SUICoinType:
		return 9
	case USDCCoinType:
		return 6
	}
	return 0
}

// func GetCoinFeedId(typeVal string) string {
// 	switch typeVal {
// 	case SUICoinType:
// 		return SUIFeefId
// 	case USDCCoinType:
// 		return USDCFeefId
// 	}
// 	return ""
// }

type CoinField struct {
	CoinType     string
	FeedId       string
	FeedObjectId string
}

func GetCoinMap() map[string]CoinField {
	coinMap := make(map[string]CoinField)
	con := common.GetDbConnection()
	sql := "select coin_type,feed_id,feed_object_id from coin_config"
	rs, err := con.Query(sql)
	if err != nil {
		fmt.Printf("GetCoinMap 失败：%v\n", err.Error())
		return coinMap
	}
	for rs.Next() {
		var c CoinField
		scanErr := rs.Scan(&c.CoinType, &c.FeedId, &c.FeedObjectId)
		if scanErr != nil {
			fmt.Printf("GetCoinMap Scan失败：%v\n", scanErr.Error())
			return coinMap
		}
		coinMap[c.CoinType] = c
	}
	defer con.Close()
	return coinMap
}

func LtvConvPercentage(val string) string {
	num, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		fmt.Printf("string转int失败：%v\n", err)
	}
	result := float64(num) / 10000000000000000
	return strconv.FormatFloat(result, 'f', 0, 64)
}

func QueryBorrowDetailRateLine(marketId string, timePeriodType int8, lineType int8) ([]BorrowRateLine, error) {
	var lines []BorrowRateLine
	dateFormat := "%m/%d %H"
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999, now.Location())
	start := end.AddDate(0, 0, -7)
	if timePeriodType == 2 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -30)
	}

	if timePeriodType == 3 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -90)
	}
	con := common.GetDbConnection()
	condition := " and rate_type in('supply')"
	if lineType == 2 {
		condition = " and rate_type in('borrow','borrow_and_supply')"
	}
	sql := "SELECT DATE_FORMAT(rd.transaction_time, '%Y-%m-%d %H:%i') transaction_time,DATE_FORMAT(rd.transaction_time,?) as dateUnit,rd.borrow_rate/10000000000000000 interestRate " +
		"FROM rate_detail rd INNER JOIN (" +
		"SELECT DATE_FORMAT(transaction_time,?) AS timeGroup, MAX(transaction_time) AS last_time FROM rate_detail " +
		"where market_id=? " + condition + " and transaction_time>=? and transaction_time<=? GROUP BY timeGroup " +
		") t ON DATE_FORMAT(transaction_time,?) = t.timeGroup AND rd.transaction_time = t.last_time"

	rs, err := con.Query(sql, dateFormat, dateFormat, marketId, start, end, dateFormat)
	if err != nil {
		return lines, fmt.Errorf(err.Error())
	}
	for rs.Next() {
		var bl BorrowRateLine
		rs.Scan(&bl.TransactionTime, &bl.DateUnit, &bl.InterestRate)
		num, _ := strconv.ParseFloat(bl.InterestRate, 64)

		log.Printf("num: %v", num)
		str1 := fmt.Sprintf("%.16f", num)
		bl.InterestRate = str1
		if err != nil {
			return nil, fmt.Errorf("扫描数据失败")
		}
		lines = append(lines, bl)
	}
	defer con.Close()
	return lines, nil
}

func QueryBorrowDetailLine(marketId string, timePeriodType int8, lineType int8) ([]BorrowLine, error) {
	var lines []BorrowLine
	dateFormat := "%m/%d %H"
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999, now.Location())
	start := end.AddDate(0, 0, -7)
	if timePeriodType == 2 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -30)
	}

	if timePeriodType == 3 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -90)
	}

	con := common.GetDbConnection()
	sql := ""
	var args []any
	args = append(args, dateFormat)
	args = append(args, marketId)
	args = append(args, start)
	args = append(args, end)
	sql = "SELECT  DATE_FORMAT(max(transaction_time), '%Y-%m-%d %H:%i') transaction_time,DATE_FORMAT(transaction_time,?) as dateUnit,sum(assets) as amount from borrow_supply_detail  where market_id=? and transaction_time>=? and transaction_time<=? GROUP BY dateUnit "
	if lineType == 2 {
		sql = "SELECT  DATE_FORMAT(max(transaction_time), '%Y-%m-%d %H:%i') transaction_time,DATE_FORMAT(transaction_time,?) as dateUnit,sum(assets) as amount from borrow_detail  where market_id=? and transaction_time>=? and transaction_time<=? GROUP BY dateUnit "
	}
	rs, err := con.Query(sql, args...)
	if err != nil {
		log.Printf("sql执行报错：%v", err.Error())
		return lines, nil
	}
	for rs.Next() {
		var bl BorrowLine
		scanErr := rs.Scan(&bl.TransactionTime, &bl.DateUnit, &bl.Amount)
		num, _ := strconv.ParseFloat(bl.Amount, 64)
		str1 := fmt.Sprintf("%.0f", num)
		bl.Amount = str1
		if scanErr != nil {
			return nil, fmt.Errorf("scanErr失败")
		}
		lines = append(lines, bl)
	}
	defer con.Close()
	return lines, nil
}

func YourTotalSupplyLine(userAddress string, timePeriodType int8) ([]BorrowLine, error) {
	var lines []BorrowLine
	dateFormat := "%m/%d %H"
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999, now.Location())
	start := end.AddDate(0, 0, -7)
	if timePeriodType == 2 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -30)
	}

	if timePeriodType == 3 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -90)
	}
	con := common.GetDbConnection()
	sql := "SELECT userMarket.*,cc.coin_type,cc.feed_id from" +
		"(SELECT DATE_FORMAT( max(transaction_time), '%Y-%m-%d %H:%i' ) AS transaction_time,DATE_FORMAT(transaction_time,?) AS dateUnit,sum( assets ) AS amount,market_id FROM borrow_supply_detail " +
		"WHERE supply_type =? AND caller_address =? and transaction_time>=? and transaction_time<=? GROUP BY dateUnit,market_id) userMarket " +
		"left join borrow b on b.market_id=userMarket.market_id " +
		"left join coin_config cc on cc.coin_type=b.collateral_token_type ORDER BY userMarket.dateUnit"
	withdrawSql := "SELECT userMarket.*,cc.feed_id  from ( " +
		"SELECT DATE_FORMAT( max( transaction_time ), '%Y-%m-%d %H:%i' ) AS transaction_time,DATE_FORMAT(transaction_time,?) AS dateUnit,sum( assets ) AS amount, " +
		"market_id,max(collateral_token_type) as collateral_token_type FROM borrow_withdraw_collateral " +
		"WHERE caller_address =? and transaction_time>=? and transaction_time<=? GROUP BY dateUnit, market_id)  userMarket " +
		"LEFT JOIN coin_config cc ON cc.coin_type = userMarket.collateral_token_type ORDER BY userMarket.dateUnit"
	// log.Printf("YourTotalSupplyLineSql=%v\n", sql)
	rs, err := con.Query(sql, dateFormat, 2, userAddress, start, end)
	withdrawRs, withdrawErr := con.Query(withdrawSql, dateFormat, userAddress, start, end)
	if err != nil {
		log.Printf("sql执行报错：%v", err.Error())
		return lines, nil
	}
	if withdrawErr != nil {
		log.Printf("withdrawSql执行报错：%v", withdrawErr.Error())
		return lines, nil
	}
	var userMarketTotals []UserTotalCollateraModel
	var userWithdrawTotals []UserTotalCollateraModel
	feedIds := make(map[string]string)
	for rs.Next() {
		var u UserTotalCollateraModel
		scanErr := rs.Scan(&u.TransactionTime, &u.DateUnit, &u.Amount, &u.MarketId, &u.CoinType, &u.FeedId)
		num, _ := strconv.ParseFloat(u.Amount, 64)
		str1 := fmt.Sprintf("%.0f", num)
		u.Amount = str1
		if scanErr != nil {
			return nil, fmt.Errorf("scanErr失败")
		}
		feedIds[u.FeedId] = u.FeedId
		userMarketTotals = append(userMarketTotals, u)
	}
	for withdrawRs.Next() {
		var u UserTotalCollateraModel
		scanErr := withdrawRs.Scan(&u.TransactionTime, &u.DateUnit, &u.Amount, &u.MarketId, &u.CoinType, &u.FeedId)
		num, _ := strconv.ParseFloat(u.Amount, 64)
		str1 := fmt.Sprintf("%.0f", num)
		u.Amount = str1
		if scanErr != nil {
			return nil, fmt.Errorf("scanErr失败")
		}
		feedIds[u.FeedId] = u.FeedId
		userWithdrawTotals = append(userWithdrawTotals, u)
	}
	dateMaps := make(map[string]BorrowLine)
	dateWithdrawMaps := make(map[string]BorrowLine)
	coinPrice := PythPrice(feedIds)
	supplyCollateralSum := 0.00
	withdrawCollateralSum := 0.00
	for _, um := range userMarketTotals {
		var b BorrowLine
		pythCoinFeedPrice := coinPrice[um.FeedId]
		usdUnitPrice := FeedIdUsdUnitPrice(pythCoinFeedPrice)
		floatAmountVal := CalculateCoinDecimalFloat(um.Amount, um.CoinType)
		coinUsdAmount := floatAmountVal * usdUnitPrice
		supplyCollateralSum += coinUsdAmount
		b.Amount = fmt.Sprintf("%.2f", supplyCollateralSum)
		b.TotalAmount = fmt.Sprintf("%.2f", supplyCollateralSum)
		b.DateUnit = um.DateUnit
		b.TransactionTime = um.TransactionTime
		dateData, ok := dateMaps[um.DateUnit]
		if !ok {
			dateMaps[um.DateUnit] = b
		} else {
			dateData.Amount = fmt.Sprintf("%.2f", supplyCollateralSum)
			dateMaps[um.DateUnit] = dateData
		}
	}

	for _, umw := range userWithdrawTotals {
		var b BorrowLine
		pythCoinFeedPrice := coinPrice[umw.FeedId]
		usdUnitPrice := FeedIdUsdUnitPrice(pythCoinFeedPrice)
		floatAmountVal := CalculateCoinDecimalFloat(umw.Amount, umw.CoinType)
		coinUsdAmount := floatAmountVal * usdUnitPrice
		withdrawCollateralSum += coinUsdAmount
		b.Amount = fmt.Sprintf("%.2f", withdrawCollateralSum)
		b.TotalAmount = fmt.Sprintf("%.2f", withdrawCollateralSum)
		b.DateUnit = umw.DateUnit
		b.TransactionTime = umw.TransactionTime
		dateData, ok := dateWithdrawMaps[umw.DateUnit]
		if !ok {
			dateWithdrawMaps[umw.DateUnit] = b
		} else {
			dateData.Amount = fmt.Sprintf("%.2f", withdrawCollateralSum)
			dateWithdrawMaps[umw.DateUnit] = dateData
		}
	}

	for _, bl := range dateMaps {
		withdraw, ok := dateWithdrawMaps[bl.DateUnit]
		if !ok {
			log.Printf("%v时间没有取抵押\n", bl.DateUnit)
		} else {
			supplyCollateralVal, _ := strconv.ParseFloat(bl.Amount, 64)
			withdrawCollateralVal, _ := strconv.ParseFloat(withdraw.Amount, 64)
			val := supplyCollateralVal - withdrawCollateralVal
			bl.Amount = fmt.Sprintf("%.2f", val)
		}
		lines = append(lines, bl)
		delete(dateWithdrawMaps, bl.DateUnit)
	}

	supplyKeys := SupplyCollateralTimeStrSortDesc(dateMaps)
	for _, wl := range dateWithdrawMaps {
		newLessThanTimeStr := TagerNewLessThanKey(wl.TransactionTime, supplyKeys)
		targetLayout := "01/02 15" // go固定日期转换格式
		time, _ := parseTimeKey(newLessThanTimeStr)
		targetStr := time.Format(targetLayout)
		supplyO, ok := dateMaps[targetStr]
		if !ok {
			log.Println("YourTotalSupplyLine supplyDate=%v\n", targetStr)
		} else {
			supplyCollateralVal, _ := strconv.ParseFloat(supplyO.TotalAmount, 64)
			withdrawCollateralVal, _ := strconv.ParseFloat(wl.Amount, 64)
			log.Printf("supplyCollateralVal=%v", fmt.Sprintf("%.2f", supplyCollateralVal))
			log.Printf("withdrawCollateralVal=%v", fmt.Sprintf("%.2f", withdrawCollateralVal))
			val := supplyCollateralVal - withdrawCollateralVal
			wl.Amount = fmt.Sprintf("%.2f", val)
			lines = append(lines, wl)
		}
	}
	sort.Slice(lines, func(i, j int) bool {
		ti, _ := time.Parse(DateFormatYMDHm, lines[i].TransactionTime)
		tj, _ := time.Parse(DateFormatYMDHm, lines[j].TransactionTime)
		return ti.Before(tj) // 升序：i时间早于j则i排在前面
	})
	defer con.Close()
	return lines, nil
}

// 存入抵押日期倒序排序
func SupplyCollateralTimeStrSortDesc(dateMaps map[string]BorrowLine) []string {
	supplyKeys := make([]string, 0, len(dateMaps))
	for _, supplyObj := range dateMaps {
		supplyKeys = append(supplyKeys, supplyObj.TransactionTime)
	}

	// 按时间顺序排序键（解析为time.Time后比较）
	sort.Slice(supplyKeys, func(i, j int) bool {
		t1, _ := parseTimeKey(supplyKeys[i])
		t2, _ := parseTimeKey(supplyKeys[j])
		return t2.Before(t1)
	})
	return supplyKeys
}

func parseTimeKey(key string) (time.Time, error) {
	// 对应YYYY-MM-DD HH:MM的时间布局
	return time.Parse(DateFormatYMDHm, key)
}

func TagerNewLessThanKey(targetKey string, supplyKeys []string) string {
	resultKey := ""
	targetTime, _ := parseTimeKey(targetKey)
	for _, key := range supplyKeys {
		keyTime, _ := parseTimeKey(key)
		if keyTime.Before(targetTime) {
			resultKey = key
			break // 找到第一个符合条件的键
		}
	}
	return resultKey
}

type TotalCollateralBorrowVo struct {
	TotalCollatera     string //所有market池子抵押总额
	TotalSupply        string //所有market池子存入总额
	TotalBorrow        string //所有market池子借总额
	UserTotalSupply    string //用户market池子存入总额
	UserTotalCollatera string //用户market池子抵押总额
	UserTotalBorrow    string //用户所有market池子抵押总额
	CollateraCoinType  string
	CollateraFeedId    string
	LoanCoinType       string
	LoanFeedId         string
	CoinType           string
	FeedId             string
}

func TotalCollateralBorrow(userAddress string) (TotalCollateralBorrowVo, error) {
	var vo TotalCollateralBorrowVo
	totalBorrowUsd := 0.00
	userTotalBorrowUsd := 0.00

	totalCollateraUsd := 0.00
	userTotalCollateraUsd := 0.00

	totalSupplyUsd := 0.00
	userTotalSupplyUsd := 0.00

	con := common.GetDbConnection()
	// sql := "select b.total_supply_amount,b.total_supply_collateral_amount,b.total_loan_amount,b.collateral_token_type,b.loan_token_type,ccc.feed_id as collateral_feed_id,ccl.feed_id as loan_feed_id from borrow b" +
	// 	"left join coin_config ccc on ccc.coin_type=b.collateral_token_type" +
	// 	"left join coin_config ccl on ccl.coin_type=b.loan_token_type "
	collateralBaseSql := "SELECT market_id,IFNULL(SUM(assets), 0) AS total_collateral_amount,SUM(CASE WHEN caller_address = ? THEN assets ELSE 0 " +
		"END) AS user_total_collateral_amount FROM borrow_supply_detail WHERE supply_type =? GROUP BY market_id"
	collateralSql := "select SUM(total_collateral_amount) total_collateral_amount," +
		"SUM(user_total_collateral_amount) user_total_collateral_amount,cc.coin_type,cc.feed_id from (" + collateralBaseSql + ") totalMarket " +
		"left join borrow b on b.market_id=totalMarket.market_id "
	collateralCoinSql := collateralSql + "left join coin_config cc on cc.coin_type=b.collateral_token_type GROUP BY coin_type,feed_id"
	supplyCoinSql := collateralSql + "left join coin_config cc on cc.coin_type=b.loan_token_type GROUP BY coin_type,feed_id"
	borrowBaseSql := "SELECT market_id,IFNULL(SUM(assets), 0) AS total_collateral_amount,SUM(CASE WHEN caller_address =? THEN assets ELSE 0  END) AS user_total_collateral_amount FROM borrow_detail GROUP BY market_id"
	borrowSql := "select SUM(total_collateral_amount) total_collateral_amount," +
		"SUM(user_total_collateral_amount) user_total_collateral_amount,cc.coin_type,cc.feed_id from (" + borrowBaseSql + ") totalMarket " +
		"left join borrow b on b.market_id=totalMarket.market_id " +
		"left join coin_config cc on cc.coin_type=b.loan_token_type " +
		"GROUP BY coin_type,feed_id"

	collateralRs, collateralErr := con.Query(collateralCoinSql, userAddress, 2)
	supplyRs, supplyErr := con.Query(supplyCoinSql, userAddress, 1)
	borrowRs, borrowErr := con.Query(borrowSql, userAddress)
	// rs, err := con.Query(sql)
	// if err != nil {
	// 	log.Printf("market查询报错：%v", err.Error())
	// 	return vo, nil
	// }
	if supplyErr != nil {
		log.Printf("supply查询报错：%v", supplyErr.Error())
		return vo, nil
	}
	if collateralErr != nil {
		log.Printf("collateral查询报错：%v", collateralErr.Error())
		return vo, nil
	}
	if borrowErr != nil {
		log.Printf("borrow查询报错：%v", borrowErr.Error())
		return vo, nil
	}
	feedIds := make(map[string]string)
	var borrowVo []TotalCollateralBorrowVo
	var collateralVo []TotalCollateralBorrowVo
	var supplyVo []TotalCollateralBorrowVo

	// for rs.Next() {
	// 	var m TotalCollateralBorrowVo
	// 	err := rs.Scan(&m.TotalSupply, &m.TotalCollatera, &m.TotalBorrow, &borrow.FeedId)
	// 	borrow.TotalBorrow = CalculateCoinDecimal(borrow.TotalBorrow, borrow.CoinType)
	// 	borrow.UserTotalBorrow = CalculateCoinDecimal(borrow.UserTotalBorrow, borrow.CoinType)
	// 	feedIds[borrow.FeedId] = borrow.FeedId
	// 	borrowVo = append(borrowVo, borrow)
	// 	if err != nil {
	// 		log.Printf("borrow赋值报错：%v", err.Error())
	// 		return vo, nil
	// 	}
	// }

	for borrowRs.Next() {
		var borrow TotalCollateralBorrowVo
		err := borrowRs.Scan(&borrow.TotalBorrow, &borrow.UserTotalBorrow, &borrow.CoinType, &borrow.FeedId)
		borrow.TotalBorrow = CalculateCoinDecimal(borrow.TotalBorrow, borrow.CoinType)
		borrow.UserTotalBorrow = CalculateCoinDecimal(borrow.UserTotalBorrow, borrow.CoinType)
		feedIds[borrow.FeedId] = borrow.FeedId
		borrowVo = append(borrowVo, borrow)
		if err != nil {
			log.Printf("borrow赋值报错：%v", err.Error())
			return vo, nil
		}
	}

	for collateralRs.Next() {
		var collateral TotalCollateralBorrowVo
		err := collateralRs.Scan(&collateral.TotalCollatera, &collateral.UserTotalCollatera, &collateral.CoinType, &collateral.FeedId)
		collateral.TotalCollatera = CalculateCoinDecimal(collateral.TotalCollatera, collateral.CoinType)
		collateral.UserTotalCollatera = CalculateCoinDecimal(collateral.UserTotalCollatera, collateral.CoinType)
		feedIds[collateral.FeedId] = collateral.FeedId
		collateralVo = append(collateralVo, collateral)
		if err != nil {
			log.Printf("collateral赋值报错：%v", err.Error())
			return vo, nil
		}
	}

	for supplyRs.Next() {
		var supply TotalCollateralBorrowVo
		err := supplyRs.Scan(&supply.TotalSupply, &supply.UserTotalSupply, &supply.CoinType, &supply.FeedId)
		supply.TotalSupply = CalculateCoinDecimal(supply.TotalSupply, supply.CoinType)
		supply.UserTotalSupply = CalculateCoinDecimal(supply.UserTotalSupply, supply.CoinType)
		feedIds[supply.FeedId] = supply.FeedId
		supplyVo = append(supplyVo, supply)
		if err != nil {
			log.Printf("supply赋值报错：%v", err.Error())
			return vo, nil
		}
	}

	coinPrice := PythPrice(feedIds)
	for _, collateralVal := range collateralVo {
		pythCoinFeedPrice := coinPrice[collateralVal.FeedId]
		usdUnitPrice := FeedIdUsdUnitPrice(pythCoinFeedPrice)

		floatTotalCollateraVal, _ := strconv.ParseFloat(collateralVal.TotalCollatera, 64)
		floatUserTotalCollateraVal, _ := strconv.ParseFloat(collateralVal.UserTotalCollatera, 64)

		coinUsdotalCollatera := floatTotalCollateraVal * usdUnitPrice
		coinUsdUserTotalCollatera := floatUserTotalCollateraVal * usdUnitPrice
		totalCollateraUsd += coinUsdotalCollatera
		userTotalCollateraUsd += coinUsdUserTotalCollatera
	}

	for _, supplyVal := range supplyVo {

		pythCoinFeedPrice := coinPrice[supplyVal.FeedId]
		usdUnitPrice := FeedIdUsdUnitPrice(pythCoinFeedPrice)
		floatTotalSupplyVal, _ := strconv.ParseFloat(supplyVal.TotalSupply, 64)
		floatUserTotalSupplyVal, _ := strconv.ParseFloat(supplyVal.UserTotalSupply, 64)

		coinUsdTotalSupply := floatTotalSupplyVal * usdUnitPrice
		coinUsdUserTotalSupply := floatUserTotalSupplyVal * usdUnitPrice
		totalSupplyUsd += coinUsdTotalSupply
		userTotalSupplyUsd += coinUsdUserTotalSupply
	}

	for _, borrowVal := range borrowVo {
		pythCoinFeedPrice := coinPrice[borrowVal.FeedId]
		usdUnitPrice := FeedIdUsdUnitPrice(pythCoinFeedPrice)

		floatTotalBorrowVal, _ := strconv.ParseFloat(borrowVal.TotalBorrow, 64)
		floatUserTotalBorrowVal, _ := strconv.ParseFloat(borrowVal.UserTotalBorrow, 64)
		coinUsdTotalBorrow := floatTotalBorrowVal * usdUnitPrice
		coinUsdUserTotalBorrowVal := floatUserTotalBorrowVal * usdUnitPrice
		totalBorrowUsd += coinUsdTotalBorrow
		userTotalBorrowUsd += coinUsdUserTotalBorrowVal

	}

	totalBorrowUsd = float64(int(totalBorrowUsd*100)) / 100 //放大 100 倍 → 取整（截断小数）→ 缩小 100 倍
	userTotalBorrowUsd = float64(int(userTotalBorrowUsd*100)) / 100

	totalCollateraUsd = float64(int(totalCollateraUsd*100)) / 100
	userTotalCollateraUsd = float64(int(userTotalCollateraUsd*100)) / 100

	totalSupplyUsd = float64(int(totalSupplyUsd*100)) / 100
	userTotalSupplyUsd = float64(int(userTotalSupplyUsd*100)) / 100

	vo.TotalBorrow = fmt.Sprintf("%.2f", totalBorrowUsd)
	vo.UserTotalBorrow = fmt.Sprintf("%.2f", userTotalBorrowUsd)

	vo.TotalCollatera = fmt.Sprintf("%.2f", totalCollateraUsd)
	vo.TotalSupply = fmt.Sprintf("%.2f", totalSupplyUsd)
	vo.UserTotalCollatera = fmt.Sprintf("%.2f", userTotalCollateraUsd)
	vo.UserTotalSupply = fmt.Sprintf("%.2f", userTotalSupplyUsd)

	defer con.Close()
	return vo, nil
}

func FeedIdUsdUnitPrice(pythCoinFeedPrice PythCoinFeedPrice) float64 {
	floatVal, _ := strconv.ParseFloat(pythCoinFeedPrice.Price, 64)
	return floatVal / math.Pow(10, float64(math.Abs(pythCoinFeedPrice.Expo)))

}

func CalculateCoinDecimal(val string, coinType string) string {
	loanCoinType := GetStringDoubleSemicolonLast(coinType)
	coinDecimal := GetCoinDecimal(loanCoinType)
	floatVal, _ := strconv.ParseFloat(ScientificComputingConvStr(val), 64)
	v := floatVal / math.Pow(10, float64(coinDecimal))
	return fmt.Sprintf("%."+fmt.Sprintf("%d", coinDecimal)+"f", v)
}

func CalculateCoinDecimalFloat(val string, coinType string) float64 {
	loanCoinType := GetStringDoubleSemicolonLast(coinType)
	coinDecimal := GetCoinDecimal(loanCoinType)
	floatVal, _ := strconv.ParseFloat(ScientificComputingConvStr(val), 64)
	return floatVal / math.Pow(10, float64(coinDecimal))
}

func ScientificComputingConvStr(val string) string {
	num, _ := strconv.ParseFloat(val, 64)
	return fmt.Sprintf("%.0f", num)
}

func BorrowSupplyDetailLine(isUser bool, userAddress string, marketId string, timePeriodType int8, lineType int8) ([]BorrowLine, error) {
	var lines []BorrowLine
	dateFormat := "%m/%d %H"
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999, now.Location())
	start := end.AddDate(0, 0, -7)
	if timePeriodType == 2 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -30)
	}

	if timePeriodType == 3 {
		dateFormat = "%m/%d"
		start = end.AddDate(0, 0, -90)
	}

	con := common.GetDbConnection()
	sql := ""
	var args []any
	args = append(args, dateFormat)
	if isUser {
		args = append(args, 2)
		args = append(args, userAddress)
		sql = "SELECT  DATE_FORMAT(max(transaction_time), '%Y-%m-%d %H:%i') transaction_time,DATE_FORMAT(transaction_time,?) as dateUnit,sum(assets) as amount from borrow_supply_detail  where supply_type=? and caller_address=?  and transaction_time>=? and transaction_time<=? GROUP BY dateUnit "
	} else {
		args = append(args, marketId)
		sql = "SELECT  DATE_FORMAT(max(transaction_time), '%Y-%m-%d %H:%i') transaction_time,DATE_FORMAT(transaction_time,?) as dateUnit,sum(assets) as amount from borrow_supply_detail  where market_id=? and transaction_time>=? and transaction_time<=? GROUP BY dateUnit "
		if lineType == 2 {
			sql = "SELECT  DATE_FORMAT(max(transaction_time), '%Y-%m-%d %H:%i') transaction_time,DATE_FORMAT(transaction_time,?) as dateUnit,sum(assets) as amount from borrow_detail  where market_id=? and transaction_time>=? and transaction_time<=? GROUP BY dateUnit "
		}
	}
	args = append(args, start)
	args = append(args, end)
	// utils.PrettyPrint(args)
	rs, err := con.Query(sql, args...)
	if err != nil {
		log.Printf("sql执行报错：%v", err.Error())
		return lines, nil
	}
	for rs.Next() {
		var bl BorrowLine
		scanErr := rs.Scan(&bl.TransactionTime, &bl.DateUnit, &bl.Amount)
		num, _ := strconv.ParseFloat(bl.Amount, 64)
		str1 := fmt.Sprintf("%.0f", num)
		bl.Amount = str1
		if scanErr != nil {
			return nil, fmt.Errorf("scanErr失败")
		}
		lines = append(lines, bl)
	}
	defer con.Close()
	return lines, nil
}

func QueryUserBorrowDetail(userAddress string, marketId string) (UserBorrowDetaliVo, error) {
	var userBorrowDetali UserBorrowDetaliVo
	con := common.GetDbConnection()
	sql := "select supply_type,assets,shares from borrow_supply_detail where caller_address=? and market_id=?"
	queryRs, queryErr := con.Query(sql, userAddress, marketId)
	if queryErr != nil {
		return userBorrowDetali, fmt.Errorf(queryErr.Error())
	}
	defer queryRs.Close()
	for queryRs.Next() {
		// queryRs.get
		var (
			supplyType string
			assets     int64
			shares     *int64
		)
		err := queryRs.Scan(&supplyType, &assets, &shares)
		if err != nil {
			return userBorrowDetali, fmt.Errorf("扫描数据失败")
		}
		if supplyType == "1" {
			userBorrowDetali.MarketSupplyAssets += assets
			if shares != nil {
				userBorrowDetali.MarketSupplyShares += *shares
			}
		}

		if supplyType == "2" {
			userBorrowDetali.MarketSupplyCollateralAssets += assets
		}
	}
	defer con.Close()
	return userBorrowDetali, nil
}

func QueryBorrowVaultList() ([]BorrowModel, error) {
	var borrows []BorrowModel
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select id,market_id,market_title,market_log,total_supply_amount,total_supply_collateral_amount,total_loan_amount,loan_token_type,collateral_token_type,fee,lltv,ltv,oracle_id,supply_rate,borrow_rate,liquidity,liquidity_proportion from borrow ")
	if queryErr != nil {
		log.Printf("QueryBorrowVaultList=11111%v\n", queryErr.Error())
		return nil, fmt.Errorf(queryErr.Error())
	}
	// defer queryRs.Close() // 延迟关闭queryRs，确保资源释放
	coinMap := GetCoinMap()
	for queryRs.Next() {
		var b BorrowModel
		err := queryRs.Scan(
			&b.Id,
			&b.MarketId,
			&b.MarketTitle,
			&b.MarketLog,
			&b.TotalSupplyAmount,
			&b.TotalSupplyCollateralAmount,
			&b.TotalLoanAmount,
			&b.LoanTokenType,
			&b.CollateralTokenType,
			&b.Fee,
			&b.Lltv,
			&b.Ltv,
			&b.OracleId,
			&b.SupplyRate,
			&b.BorrowRate,
			&b.Liquidity,
			&b.LiquidityProportion,
		)
		//todo
		// b.SupplyRate = "1.1%"
		// b.BorrowRate = "1.5%"
		// b.Liquidity = "20000000000000"
		b.Lltv = LtvConvPercentage(b.Lltv)
		b.Ltv = LtvConvPercentage(b.Ltv)
		if err != nil {
			return nil, fmt.Errorf("扫描数据失败")
		}

		collateralCoinType := GetStringDoubleSemicolonLast(b.CollateralTokenType)
		loanCoinType := GetStringDoubleSemicolonLast(b.LoanTokenType)
		b.Pair = collateralCoinType + "/" + loanCoinType
		b.CollateralCoinDecimals = GetCoinDecimal(collateralCoinType)
		b.LoanCoinDecimals = GetCoinDecimal(loanCoinType)
		collaCoin := coinMap[b.CollateralTokenType]
		loanCoin := coinMap[b.LoanTokenType]
		b.CollateralFeedId = collaCoin.FeedId
		b.CollateralFeedObjectId = collaCoin.FeedObjectId
		b.LoanFeedId = loanCoin.FeedId
		b.LoanFeedObjectId = loanCoin.FeedObjectId

		borrows = append(borrows, b)
	}
	defer con.Close() // 程序退出时关闭数据库连接
	return borrows, nil

}

func GetStringDoubleSemicolonLast(val string) string {
	lastIndx := strings.LastIndex(val, "::")
	if lastIndx == -1 {
		fmt.Println("未找到分隔符 ::")
		return ""
	}
	return val[lastIndx+2:]
}

func PythPrice(feedIds map[string]string) map[string]PythCoinFeedPrice {
	var feedPrices = make(map[string]PythCoinFeedPrice)
	params := url.Values{}
	for _, v := range feedIds {
		params.Add("ids[]", v)
	}
	baseURL := "https://hermes-beta.pyth.network/v2/updates/price/latest"

	fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())
	resp, err := http.Get(fullURL)
	if err != nil {
		// 处理请求发送失败（如网络错误、URL 非法等）
		log.Printf("PythPrice-请求发送失败：%v\n", err)
		return feedPrices
	}
	// 关键：必须关闭响应体（避免资源泄漏），使用 defer 确保函数退出时执行
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("PythPrice-关闭响应体失败：%v\n", err)
		}
	}()

	// 检查响应状态码（200 OK 表示请求成功）
	if resp.StatusCode != http.StatusOK {
		log.Printf("PythPrice-请求失败，状态码：%d\n", resp.StatusCode)
		return feedPrices
	}

	bodyBytes, err := io.ReadAll(resp.Body) // 若 Go 版本 ≥1.16，替换为 io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("PythPrice-读取响应体失败：%v\n", err)
		return feedPrices
	}

	// 转换字节为字符串，打印结果
	// bodyStr := string(bodyBytes)
	var jsonMap map[string]interface{}
	jsonErr := json.Unmarshal(bodyBytes, &jsonMap)
	if jsonErr != nil {
		log.Printf("PythPrice-JSON 解析失败：%v\n", err)
		return feedPrices
	}

	parseds, ok := jsonMap["parsed"].([]interface{})
	if !ok {
		log.Printf("PythPrice-parsed 不是数组类型\n")
		return feedPrices
	}

	for _, parsed := range parseds {
		var feedPrice PythCoinFeedPrice
		parsedMap, ok := parsed.(map[string]interface{})
		if !ok {
			log.Printf("PythPrice-parsedMap 不是map\n")
			return feedPrices
			// log.Fatalf()
		}
		priceMap, ok2 := parsedMap["price"].(map[string]interface{})
		if !ok2 {
			log.Printf("PythPrice-priceMap2 不是map\n")
			return feedPrices
		}
		feedPrice.FeedId = "0x" + parsedMap["id"].(string)
		feedPrice.Price = priceMap["price"].(string)
		feedPrice.Expo = priceMap["expo"].(float64)
		feedPrices[feedPrice.FeedId] = feedPrice
	}

	return feedPrices
}

type PythCoinFeedPrice struct {
	FeedId string
	Price  string
	Expo   float64
}

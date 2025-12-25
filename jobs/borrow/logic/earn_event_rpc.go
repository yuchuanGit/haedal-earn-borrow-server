package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"haedal-earn-borrow-server/common"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/mystenbcs"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
	"github.com/block-vision/sui-go-sdk/utils"
)

const (
	ScheduledTaskTypeBorrow = 1
	ScheduledTaskTypeVault  = 2
)

func RpcRequestScanCreateVault() {
	reqParams := SuiTransactionBlockCreateVaultParameter()
	resp := EventRpcRequest(reqParams)
	for _, data := range resp.Data {
		digest := data.Digest
		transactionTime := data.TimestampMs
		for _, event := range data.Events {
			eventType := EventType(event.Type)
			switch eventType {
			case VaultEvent: // 创建Vault
				InsertVault(event.ParsedJson, digest, transactionTime)
			}
		}
	}
}

func SuiTransactionBlockCreateVaultParameter() models.SuiXQueryTransactionBlocksRequest {
	params := models.SuiXQueryTransactionBlocksRequest{
		SuiTransactionBlockResponseQuery: models.SuiTransactionBlockResponseQuery{
			TransactionFilter: models.TransactionFilter{
				"MoveFunction": map[string]interface{}{
					"package":  PackageId,
					"module":   "meta_vault_entry",
					"function": "create",
				},
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
	return params
}

func ExecuteMoveUpdateVaultTotalAsset(vaultId string) {
	cli := sui.NewSuiClient(SuiEnv)
	ctx := context.Background()
	tx := transaction.NewTransaction()
	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(SuiUserAddress))
	arguments, parameErr := GetTotalAssetsParameter(cli, ctx, *tx, vaultId)
	if parameErr != nil {
		return
	}
	vaultInfo := QueryVaultByVaultId(vaultId)
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
	funcName := "get_total_assets"
	moveCallReturn := ExecuteDevInspectTransactionBlock(cli, ctx, *tx, moduleName, funcName, typeArguments, arguments)
	if len(moveCallReturn) > 0 {
		totalAsset := "-1"
		for _, returnValue := range moveCallReturn[0].ReturnValues {
			bcsBytes, _ := AnyToBytes(returnValue.([]any)[0])
			deserializer := bcs.NewDeserializer(bcsBytes)
			val := deserializer.U128()
			totalAsset = val.String()
		}
		if totalAsset != "-1" {
			UpdateVaultTotalAsset(totalAsset, vaultId)
		}
	}
}

func QueryVaultByVaultId(vaultId string) VaultModel {
	var vm VaultModel
	con := common.GetDbConnection()
	sql := "select vault_name,asset_type,htoken_type from vault where vault_id=?"
	err := con.QueryRow(sql, vaultId).Scan(&vm.VaultName, &vm.AssetType, &vm.HtokenType)
	if err != nil {
		log.Printf("QueryVaultByVaultId Scan失败: %v", err)
		defer con.Close()
		return vm
	}
	defer con.Close()
	return vm
}

func UpdateVaultTotalAsset(totalAsset string, vaultId string) {
	con := common.GetDbConnection()
	sql := "update vault set total_asset=? where vault_id=?"
	result, err := con.Exec(sql, totalAsset, vaultId)
	if err != nil {
		log.Printf("vault total_asset更新失败: %v", err)
		defer con.Close()
		return
	}
	updateRowCount, _ := result.RowsAffected()
	log.Printf("vault updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func ExecuteDevInspectTransactionBlock(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, moduleName string, funcName string, typeArguments []transaction.TypeTag, arguments []transaction.Argument) []moveCallResult {
	tx.MoveCall(
		models.SuiAddress(PackageId),
		moduleName,
		funcName,
		typeArguments,
		arguments,
	)
	var moveCallReturn []moveCallResult
	bcsEncodedMsg, err := tx.Data.V1.Kind.Marshal()
	txBytes := mystenbcs.ToBase64(bcsEncodedMsg)
	if err != nil {
		fmt.Printf("tx.Data.V1.Kind.Marshal查询失败：%v\n", err)
		return moveCallReturn
	}

	devRs, devErr := cli.SuiDevInspectTransactionBlock(ctx, models.SuiDevInspectTransactionBlockRequest{
		Sender:  SuiUserAddress,
		TxBytes: txBytes,
	})
	if devErr != nil {
		fmt.Printf("SuiDevInspectTransactionBlock查询失败：%v\n", devErr.Error())
		return moveCallReturn
	}
	if devRs.Effects.Status.Status == "failure" {
		fmt.Println("SuiDevInspectTransactionBlock Status 失败:", devRs.Effects.Status.Error)
		return moveCallReturn
	}
	resultsMarshalled, err2 := devRs.Results.MarshalJSON()
	if err2 != nil {
		fmt.Println("SuiDevInspectTransactionBlock MarshalJSON 失败:", err2.Error())
		return moveCallReturn
	}

	err3 := json.Unmarshal(resultsMarshalled, &moveCallReturn)
	if err3 != nil {
		fmt.Println("moveCallReturn convert json fail ", err3.Error())
		return moveCallReturn
	}
	return moveCallReturn
}

func GetTotalAssetsParameter(cli sui.ISuiAPI, ctx context.Context, tx transaction.Transaction, vaultObjectId string) ([]transaction.Argument, error) {
	valutSharedObject, err := GetSharedObjectRef(ctx, cli, vaultObjectId, true)
	hearnSharedObject, err2 := GetSharedObjectRef(ctx, cli, HEarnObjectId, true)
	log.Printf("vaultObjectId=%v\n", vaultObjectId)
	log.Printf("HEarnObjectId=%v\n", HEarnObjectId)
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

type VaultModel struct {
	VaultId           string
	VaultName         *string
	AssetType         string
	HtokenType        string
	AssetDecimals     float64 // 存入精度
	TotalShares       string  // 存入总的份额
	AssetReserve      string  // 总的闲置数量
	SupplyCap         string  // Vault 最大存入数量
	MaxDeposit        string  // Vault 单次最大存入数量
	MinDeposit        string  // Vault 单次最小存入数量
	ManagementFeeBps  string
	PerformanceFeeBps string
}

func QueryVaultInfoUpdate(vaultId string) {
	cli := sui.NewSuiClient(SuiEnv)
	ctx := context.Background()
	reqParams := models.SuiGetObjectRequest{
		ObjectId: vaultId,
		Options: models.SuiObjectDataOptions{
			ShowType:    true,
			ShowContent: true,
			ShowBcs:     false,
		},
	}
	resp, err := cli.SuiGetObject(ctx, reqParams)
	if err != nil {
		fmt.Printf("QueryVaultInfoUpdate SuiGetObject err:%v\n", err)
	}
	if resp.Data != nil {
		if resp.Data.Content != nil {
			var vm VaultModel
			vm.VaultId = vaultId
			fields := resp.Data.Content.Fields
			vault_name, _ := fields["vault_name"].(string)
			vm.VaultName = &vault_name
			vm.AssetDecimals = fields["asset_decimals"].(float64) // 存入精度
			vm.TotalShares = fields["total_shares"].(string)      // 存入总的份额
			// vm.TotalShares = fmt.Sprintf("%.0f", total_shares)
			vm.AssetReserve = fields["asset_reserve"].(string) // 总的闲置数量
			strategy := fields["strategy"].(map[string]interface{})
			fees := fields["fees"].(map[string]interface{})
			if strategy != nil {
				strategyFields := strategy["fields"].(map[string]interface{})
				vm.SupplyCap = strategyFields["supply_cap"].(string)   // Vault 最大存入数量
				vm.MaxDeposit = strategyFields["max_deposit"].(string) // Vault 单次最大存入数量
				vm.MinDeposit = strategyFields["min_deposit"].(string) // Vault 单次最小存入数量
			}
			if fees != nil {
				feesFields := fees["fields"].(map[string]interface{})
				vm.ManagementFeeBps = feesFields["management_fee_bps"].(string)   // Vault 管理费用
				vm.PerformanceFeeBps = feesFields["performance_fee_bps"].(string) // Vault 绩效费用
			}
			VaultInfoUpdate(vm)
		} else {
			fmt.Printf("QueryVaultInfoUpdate SuiGetObject resp.Data.Content:%v\n", resp.Data.Content)
		}
	} else {
		fmt.Printf("QueryVaultInfoUpdate SuiGetObject resp.Data:%v\n", resp.Data)
	}

	// res.Data.Content.Fields
}

func VaultInfoUpdate(vm VaultModel) {
	con := common.GetDbConnection()
	sql := "update vault set vault_name=?,asset_decimals=?,total_shares=?,asset_reserve=?,supply_cap=?,max_deposit=?,min_deposit=?,management_fee_bps=?,performance_fee_bps=? where vault_id=?"
	result, err := con.Exec(sql, vm.VaultName, vm.AssetDecimals, vm.TotalShares, vm.AssetReserve, vm.SupplyCap, vm.MaxDeposit, vm.MinDeposit, vm.ManagementFeeBps, vm.PerformanceFeeBps, vm.VaultId)
	if err != nil {
		log.Printf("vault_borrow_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	updateRowCount, _ := result.RowsAffected()
	log.Printf("vault updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func ScanVaultEvent() {
	jobTasks := QueryVaultExecutionInputObjectId()
	isFinalTask := false
	lastIdx := len(jobTasks) - 1
	for idx, jobTask := range jobTasks {
		if idx == lastIdx {
			isFinalTask = true
		}
		RpcRequestScanVaultEvent(jobTask, isFinalTask)
		QueryVaultInfoUpdate(jobTask.InputObjectId)
		ExecuteMoveUpdateVaultTotalAsset(jobTask.InputObjectId)
	}
	// if len(jobTasks) == 1 {
	// 	fmt.Printf("ScanVaultEvent最后一条：%v\n", jobTasks[0].InputObjectId)
	// 	RpcRequestScanVaultEvent(jobTasks[0], true)
	// 	QueryVaultInfoUpdate(jobTasks[0].InputObjectId)
	// 	ExecuteMoveUpdateVaultTotalAsset(jobTasks[0].InputObjectId)
	// } else if len(jobTasks) > 1 {
	// 	RpcRequestScanVaultEvent(jobTasks[0], false)
	// 	QueryVaultInfoUpdate(jobTasks[0].InputObjectId)
	// 	ExecuteMoveUpdateVaultTotalAsset(jobTasks[0].InputObjectId)
	// }
}

func RpcRequestScanVaultEvent(jobInfo ScheduledTaskRecord, isFinalTask bool) {
	nextCursor := ""
	if jobInfo.Digest != nil {
		nextCursor = *jobInfo.Digest
	}
	reqParams := SuiTransactionBlockVaultEventParameter(jobInfo.InputObjectId, nextCursor)
	resp := EventRpcRequest(reqParams)
	EventsCursorUpdateById(resp.NextCursor, jobInfo.Id)
	if len(resp.Data) == 0 {
		log.Printf("RpcRequestScanVaultEvent lastCursor=%v\n", nextCursor)
		if isFinalTask {
			UpdateTimingTypeExecutionCompleted(false, ScheduledTaskTypeVault, 0) // 更新所有Vault任务未执行
		} else {
			UpdateTimingTypeExecutionCompleted(true, ScheduledTaskTypeVault, jobInfo.Id) // 更新所有Vault任务已完成执行
		}
		return
	}
	for _, data := range resp.Data {
		digest := data.Digest
		transactionTimeUnix := data.TimestampMs
		for _, event := range data.Events {
			eventType := EventType(event.Type)
			switch eventType {
			case SetAllocationEvent: // 设置cap
				SetAllocationCap(event.ParsedJson, digest, transactionTimeUnix)
			case SetSupplyQueueEvent: // 设置存款队列，Vault和Market相关联
				InsertVaultSupplyQueue(event.ParsedJson, digest, transactionTimeUnix)
			case SetWithdrawQueueEvent: // 设置取款队列，Vault和Market相关联
				InsertWithdrawSupplyQueue(event.ParsedJson, digest, transactionTimeUnix)
			case SetOwnerEvent: // Vault设置更新owner记录
				InsertVaultSetOwnerRecord(event.ParsedJson, digest, transactionTimeUnix)
			case SetCuratorEvent: // Vault设置更新Curator记录
				InsertVaultSetCuratorRecord(event.ParsedJson, digest, transactionTimeUnix)
			case SetAllocatorEvent: // Vault设置更新Allocator记录
				InsertVaultSetAllocatorRecord(event.ParsedJson, digest, transactionTimeUnix)
			case SubmitTimelockEvent: // 提交时间锁记录
				InsertVaultSubmitTimeLock(event.ParsedJson, digest, transactionTimeUnix)
			case SetGuardianEvent: // 设置更新Guardian记录
				InsertVaultSetGuardian(event.ParsedJson, digest, transactionTimeUnix)
			case RevokePendingEvent: // Vault撤销待定记录
				InsertVaultRevokePending(event.ParsedJson, digest, transactionTimeUnix)
			case SubmitSupplyCapEvent: // vault提交生效cap
				InsertVaultSubmitSupplyCap(event.ParsedJson, digest, transactionTimeUnix)
			case ApplySupplyCapEvent: // vault应用存入上限cap
				InsertVaultApplySupplyCap(event.ParsedJson, digest, transactionTimeUnix)
			case SubmitMarketRemovalEvent: // vault提交移除Market
				InsertVaultSubmitMarketRemoval(event.ParsedJson, digest, transactionTimeUnix)
			case RemoveMarketEvent: // vault提交移除Market
				InsertVaultRemoveMarket(event.ParsedJson, digest, transactionTimeUnix)
			case SetMinDepositEvent: // vault设置最小押金
				InsertVaultSetMinDeposit(event.ParsedJson, digest, transactionTimeUnix)
			case SetMaxDepositEvent: // vault设置最大押金
				InsertVaultSetMaxDeposit(event.ParsedJson, digest, transactionTimeUnix)
			case SetWithdrawCooldownEvent: // vault设置提款冷却事件
				InsertVaultSetWithdrawCooldown(event.ParsedJson, digest, transactionTimeUnix)
			case SetMinRebalanceIntervalEvent: // 设置最小再平衡间隔
				InsertVaultSetMinRebalanceInterval(event.ParsedJson, digest, transactionTimeUnix)
			case UpdateLastRebalanceEvent: // 更新上次重新平衡事件
				InsertVaultUpdateLastRebalance(event.ParsedJson, digest, transactionTimeUnix)
			case SetFeeRecipientEvent: // 设置费用接收人
				InsertVaultSetFeeRecipient(event.ParsedJson, digest, transactionTimeUnix)
			case SubmitPerformanceFeeEvent: // 提交绩效费
				InsertVaultSubmitPerformanceFee(event.ParsedJson, digest, transactionTimeUnix)
			case ApplyPerformanceFeeEvent: // 申请绩效费
				InsertVaultApplyPerformanceFee(event.ParsedJson, digest, transactionTimeUnix)
			case SubmitManagementFeeEvent: // 提交管理费
				InsertVaultSubmitmentFee(event.ParsedJson, digest, transactionTimeUnix)
			case ApplyManagementFeeEvent: // 申请管理费
				InsertVaultApplyManagementFee(event.ParsedJson, digest, transactionTimeUnix)
			case VaultDepositEvent: // 用户存入Vault池
				InsertVaultDeposit(event.ParsedJson, digest, transactionTimeUnix)
			case VaultWithdrawEvent: // 用户取出Vault池
				InsertVaultWithdraw(event.ParsedJson, digest, transactionTimeUnix)
			case SetVaultNameEvent: // 设置Vault名称
				InsertVaultSetName(event.ParsedJson, digest, transactionTimeUnix)
			case CompensateLostAssetsEvent: // Vault补偿损失资产
				InsertVaultCompensateLostAssets(event.ParsedJson, digest, transactionTimeUnix)
			case AccrueFeesEvent: // Vault池应计费用
				InsertVaultAccrueFees(event.ParsedJson, digest, transactionTimeUnix)
			case RebalanceEvent: // Vault池Rebalance
				InsertVaultRebalance(event.ParsedJson, digest, transactionTimeUnix)
			}
		}
	}
	RpcRequestScanVaultEvent(jobInfo, isFinalTask)
}

func SuiTransactionBlockVaultEventParameter(vaultObjectId string, nextCursor string) models.SuiXQueryTransactionBlocksRequest {
	params := models.SuiXQueryTransactionBlocksRequest{
		SuiTransactionBlockResponseQuery: models.SuiTransactionBlockResponseQuery{
			TransactionFilter: models.TransactionFilter{
				"InputObject": vaultObjectId,
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

func UpdateTimingTypeExecutionCompleted(executionCompleted bool, timingType int, id int) {
	con := common.GetDbConnection()
	sql := "update scheduled_task_record set execution_completed=? where timing_type=?"
	var params []any
	params = append(params, executionCompleted)
	params = append(params, timingType)
	if executionCompleted {
		sql = sql + " and id=?"
		params = append(params, id)
	}
	_, upErr := con.Exec(sql, params...)
	if upErr != nil {
		fmt.Printf("UpdateTimingTypeExecutionCompleted scheduled_task_record execution_completed=0失败：%v\n", upErr.Error())
	}
	defer con.Close()
}

func EventsCursorUpdateById(digest string, id int) {
	if digest == "" || digest == "null" || digest == "undefined" {
		return
	}
	con := common.GetDbConnection()
	sql := "update scheduled_task_record set digest=? where id=?"
	rs, err := con.Exec(sql, digest, id)
	if err != nil {
		log.Printf("scheduled_task_record update digest失败：%v\n", err)
	}
	updateRowCount, _ := rs.RowsAffected()
	log.Printf("scheduled_task_record updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func QueryVaultExecutionInputObjectId() []ScheduledTaskRecord {
	var jobTasks []ScheduledTaskRecord
	con := common.GetDbConnection()
	// sql := "SELECT id,digest,input_object_id from scheduled_task_record where timing_type=? and execution_completed=?"
	// rs, err := con.Query(sql, ScheduledTaskTypeVault, false)
	sql := "SELECT id,digest,input_object_id from scheduled_task_record where timing_type=?"
	rs, err := con.Query(sql, ScheduledTaskTypeVault)
	if err != nil {
		fmt.Printf("QueryVaultExecutionInputObjectId 查询borrow失败：%v\n", err.Error())
		defer con.Close()
		return jobTasks
	}
	for rs.Next() {
		var tr ScheduledTaskRecord
		rs.Scan(&tr.Id, &tr.Digest, &tr.InputObjectId)
		jobTasks = append(jobTasks, tr)
	}
	defer con.Close()
	return jobTasks
}

type ScheduledTaskRecord struct {
	Id                 int
	TimingType         string  // 定时类型 1 borrow事件扫描 2.vault事件扫描'
	Digest             *string // 下一次扫描事件游标
	InputObjectId      string  // 扫描交易合约函数入参id
	ExecutionCompleted bool    // 扫描是否完成 0 为完成 1 完成
}

func EventRpcRequest(req models.SuiXQueryTransactionBlocksRequest) models.SuiXQueryTransactionBlocksResponse {
	var response models.SuiXQueryTransactionBlocksResponse
	cli := sui.NewSuiClient(SuiBlockvisionEnv)
	ctx := context.Background()
	resp, err := cli.SuiXQueryTransactionBlocks(ctx, req)
	if err != nil {
		fmt.Printf("RpcApiRequest err:%v\n", err)
		return response
	}
	return resp
}

func EventType(typeStr string) string {
	resultType := ""
	delimiter := "::"
	parts := strings.Split(typeStr, "::")
	if len(parts) > 1 {
		// 拼接分割后的后续部分（从第一个::之后开始）
		resultType = delimiter + strings.Join(parts[1:], delimiter)
	} else {
		log.Println("无" + delimiter + "分隔符")
	}
	return resultType
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
	log.Printf("isInsertTask：=%v", isInsertTask)
	taskRs, taskErr := con.Query("select * from scheduled_task_record where input_object_id=?", vault_id)
	if taskErr != nil {
		log.Printf("scheduled_task_record查询 input_object_id失败: %v", taskErr)
		defer con.Close()
		return
	}
	if taskRs.Next() {
		log.Printf("1111111=%v", isInsertTask)
		isInsertTask = false
	}
	if isInsertTask {
		sqlTask := "insert into scheduled_task_record(timing_type,input_object_id,execution_completed) value(?,?,?)"
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

func InsertVaultRebalance(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_rebalance where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_rebalance查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_rebalance digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_rebalance(vault_id,caller,total_assets_before,total_assets_after,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, total_assets_before, total_assets_after, asset_type, htoken_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_rebalance新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_rebalance新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultAccrueFees(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	management_fee_shares := parsedJson["management_fee_shares"].(string)
	performance_fee_shares := parsedJson["performance_fee_shares"].(string)
	last_total_assets := parsedJson["last_total_assets"].(string)
	management_fee_assets := parsedJson["management_fee_assets"].(string)
	performance_fee_assets := parsedJson["performance_fee_assets"].(string)
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_accrue_fees where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_accrue_fees查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_accrue_fees digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_accrue_fees(vault_id,management_fee_shares,performance_fee_shares,last_total_assets,management_fee_assets,performance_fee_assets,total_shares_minted,total_assets,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, management_fee_shares, performance_fee_shares, last_total_assets, management_fee_assets, performance_fee_assets, total_shares_minted, total_assets, asset_type, htoken_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_accrue_fees新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_accrue_fees新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultCompensateLostAssets(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_compensate_lost_assets where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_compensate_lost_assets查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_compensate_lost_assets digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_compensate_lost_assets(vault_id,caller,amount,remaining_lost,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, amount, remaining_lost, asset_type, htoken_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_compensate_lost_assets新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_compensate_lost_assets新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetName(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_set_name(vault_id,caller,old_name,new_name,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, old_name, new_name, asset_type, htoken_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_name新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_name新增id：=%v", lastInsertID)
	// todo vault 表更新最新名称
	defer con.Close()
}

func InsertVaultWithdraw(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_withdraw where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_withdraw查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_withdraw digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_withdraw(vault_id,user,asset_amount,shares_burned,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, user, asset_amount, shares_burned, asset_type, htoken_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_withdraw新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_withdraw新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultDeposit(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_deposit where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_deposit查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_deposit digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_deposit(vault_id,user,asset_amount,shares_minted,asset_type,htoken_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, user, asset_amount, shares_minted, asset_type, htoken_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_deposit新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_deposit新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitmentFee(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	floatVal, ok := parsedJson["event_type"].(float64)
	if !ok || floatVal < 0 || floatVal > 255 {
		log.Printf("event_type 解析失败：值=%v, 类型=%T", parsedJson["event_type"], parsedJson["event_type"])
	}
	event_type := uint8(floatVal)
	if event_type == 2 || event_type == 3 || event_type == 4 {
		timestampMsUnix := parsedJson["timestamp_ms"].(string)
		convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
		if convErr != nil {
			log.Printf("转换失败：%v\n", convErr)
		}
		timestampMs := time.UnixMilli(convRs)
		ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
		if ttConvErr != nil {
			log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
		}
		transactionTime := time.UnixMilli(ttConvRs)
		con := common.GetDbConnection()
		queryRs, queryErr := con.Query("select * from vault_submit_management_fee where digest=? and vault_id=?", digest, vaultId)
		if queryErr != nil {
			log.Printf("vault_submit_management_fee查询 digest+vault_id失败: %v", queryErr)
			defer con.Close()
			return
		}
		if queryRs.Next() {
			fmt.Printf("vault_submit_management_fee digest+vault_id exist :%v,%v\n", digest, vaultId)
			defer queryRs.Close()
			defer con.Close()
			return
		}

		sql := "insert into vault_submit_management_fee(vault_id,caller,fee_bps,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
		result, err := con.Exec(sql, vaultId, caller, fee_bps, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
		if err != nil {
			log.Printf("vault_submit_management_fee新增失败: %v", err)
			defer con.Close()
			return
		}
		lastInsertID, _ := result.LastInsertId()
		log.Printf("vault_submit_management_fee新增id：=%v", lastInsertID)
		defer con.Close()
	}
}

func InsertVaultSubmitPerformanceFee(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	floatVal, ok := parsedJson["event_type"].(float64)
	if !ok || floatVal < 0 || floatVal > 255 {
		log.Printf("event_type 解析失败：值=%v, 类型=%T", parsedJson["event_type"], parsedJson["event_type"])
	}
	event_type := uint8(floatVal)
	if event_type == 2 || event_type == 3 || event_type == 4 {
		timestampMsUnix := parsedJson["timestamp_ms"].(string)
		convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
		if convErr != nil {
			log.Printf("转换失败：%v\n", convErr)
		}
		timestampMs := time.UnixMilli(convRs)
		ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
		if ttConvErr != nil {
			log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
		}
		transactionTime := time.UnixMilli(ttConvRs)
		con := common.GetDbConnection()
		queryRs, queryErr := con.Query("select * from vault_submit_performance_fee where digest=? and vault_id=?", digest, vaultId)
		if queryErr != nil {
			log.Printf("vault_submit_performance_fee查询 digest+vault_id失败: %v", queryErr)
			defer con.Close()
			return
		}
		if queryRs.Next() {
			fmt.Printf("vault_submit_performance_fee digest+vault_id exist :%v,%v\n", digest, vaultId)
			defer queryRs.Close()
			defer con.Close()
			return
		}

		sql := "insert into vault_submit_performance_fee(vault_id,caller,fee_bps,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
		result, err := con.Exec(sql, vaultId, caller, fee_bps, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
		if err != nil {
			log.Printf("vault_submit_performance_fee新增失败: %v", err)
			defer con.Close()
			return
		}
		lastInsertID, _ := result.LastInsertId()
		log.Printf("vault_submit_performance_fee新增id：=%v", lastInsertID)
		defer con.Close()
	}
}

func InsertVaultApplyManagementFee(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_apply_management_fee(vault_id,caller,fee_bps,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, fee_bps, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_apply_management_fee新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apply_management_fee新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultApplyPerformanceFee(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	fee_bps := parsedJson["fee_bps"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_apply_performance_fee(vault_id,caller,fee_bps,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, fee_bps, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_apply_performance_fee新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apply_performance_fee新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetFeeRecipient(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_set_fee_recipient(vault_id,caller,previous_fee_recipient,new_fee_recipient,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_fee_recipient, new_fee_recipient, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_fee_recipient新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_fee_recipient新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultUpdateLastRebalance(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	rebalance_time := parsedJson["rebalance_time"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_updatelast_rebalance(vault_id,caller,rebalance_time,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, rebalance_time, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_updatelast_rebalance新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_updatelast_rebalance新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetMinRebalanceInterval(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	interval := parsedJson["interval"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_set_min_rebalance_interval(vault_id,caller,interval,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, interval, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_min_rebalance_interval新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_min_rebalance_interval新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetWithdrawCooldown(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	cooldown_ms := parsedJson["cooldown_ms"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_set_withdraw_cooldown(vault_id,caller,cooldown_ms,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, cooldown_ms, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_withdraw_cooldown新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_withdraw_cooldown新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetMaxDeposit(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	max_deposit := parsedJson["max_deposit"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_set_max_deposit(vault_id,caller,max_deposit,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, max_deposit, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_max_deposit新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_max_deposit新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetMinDeposit(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	min_deposit := parsedJson["min_deposit"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_set_min_deposit(vault_id,caller,min_deposit,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, min_deposit, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_min_deposit新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_min_deposit新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitSupplyCap(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	new_cap := parsedJson["new_cap"].(string)
	valid_at_ms := parsedJson["valid_at_ms"].(string)
	floatVal, ok := parsedJson["event_type"].(float64)
	if !ok || floatVal < 0 || floatVal > 255 {
		log.Printf("event_type 解析失败：值=%v, 类型=%T", parsedJson["event_type"], parsedJson["event_type"])
	}
	event_type := uint8(floatVal)
	if event_type == 2 || event_type == 3 || event_type == 4 {
		timestampMsUnix := parsedJson["timestamp_ms"].(string)
		convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
		if convErr != nil {
			log.Printf("转换失败：%v\n", convErr)
		}
		timestampMs := time.UnixMilli(convRs)
		ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
		if ttConvErr != nil {
			log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
		}
		transactionTime := time.UnixMilli(ttConvRs)
		con := common.GetDbConnection()
		queryRs, queryErr := con.Query("select * from vault_submit_supply_cap where digest=? and vault_id=?", digest, vaultId)
		if queryErr != nil {
			log.Printf("vault_submit_supply_cap查询 digest+vault_id失败: %v", queryErr)
			defer con.Close()
			return
		}
		if queryRs.Next() {
			fmt.Printf("vault_submit_supply_cap digest+vault_id exist :%v,%v\n", digest, vaultId)
			defer queryRs.Close()
			defer con.Close()
			return
		}

		sql := "insert into vault_submit_supply_cap(vault_id,caller,new_cap,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
		result, err := con.Exec(sql, vaultId, caller, new_cap, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
		if err != nil {
			log.Printf("vault_submit_supply_cap新增失败: %v", err)
			defer con.Close()
			return
		}
		lastInsertID, _ := result.LastInsertId()
		log.Printf("vault_submit_supply_cap新增id：=%v", lastInsertID)
		defer con.Close()
	}
}

func InsertVaultApplySupplyCap(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	new_cap := parsedJson["new_cap"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_apply_supply_cap(vault_id,caller,new_cap,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, new_cap, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_apply_supply_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_apply_supply_cap新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSubmitMarketRemoval(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_submit_market_removal(vault_id,caller,market_id,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, market_id, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_submit_market_removal新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_market_removal新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultRemoveMarket(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_remove_market(vault_id,caller,market_id,is_emergency,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, market_id, is_emergency, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_remove_market新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_remove_market新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultRevokePending(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	pending_type := parsedJson["pending_type"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_revoke_pending(vault_id,caller,pending_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, pending_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_revoke_pending新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_revoke_pending新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetGuardian(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	previous_guardian := parsedJson["previous_guardian"].(string)
	new_guardian := parsedJson["new_guardian"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_guardian_record where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_set_guardian_record查询 digest+vaultId失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_guardian_record digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_guardian_record(vault_id,caller,previous_guardian,new_guardian,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_guardian, new_guardian, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_guardian_record新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_guardian_record新增id：=%v", lastInsertID)
	upVaultSql := "update vault set guardian=? where vault_id=?"
	rsUp, errUp := con.Exec(upVaultSql, new_guardian, vaultId)
	if errUp != nil {
		log.Printf("vault guardian更新失败: %v", errUp)
		defer con.Close()
		return
	}
	updateRowCount, _ := rsUp.RowsAffected()
	log.Printf("vault updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func InsertVaultSubmitTimeLock(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
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

	sql := "insert into vault_submit_time_lock(vault_id,caller,new_timelock_minutes,valid_at_ms,event_type,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, new_timelock_minutes, valid_at_ms, event_type, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_submit_time_lock新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_submit_time_lock新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSetAllocatorRecord(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_allocator_record where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_set_allocator_record查询 digest+vaultId失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_allocator_record digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_allocator_record(vault_id,caller,previous_allocator,new_allocator,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_allocator, new_allocator, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_allocator_record新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_allocator_record新增id：=%v", lastInsertID)
	upVaultSql := "update vault set allocator=? where vault_id=?"
	rsUp, errUp := con.Exec(upVaultSql, new_allocator, vaultId)
	if errUp != nil {
		log.Printf("vault allocator更新失败: %v", errUp)
		defer con.Close()
		return
	}
	updateRowCount, _ := rsUp.RowsAffected()
	log.Printf("vault updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func InsertVaultSetOwnerRecord(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
	vaultId := parsedJson["vault_id"].(string)
	caller := parsedJson["caller"].(string)
	previous_owner := parsedJson["previous_owner"].(string)
	new_owner := parsedJson["new_owner"].(string)
	timestampMsUnix := parsedJson["timestamp_ms"].(string)
	convRs, convErr := strconv.ParseInt(timestampMsUnix, 10, 64)
	if convErr != nil {
		log.Printf("转换失败：%v\n", convErr)
	}
	timestampMs := time.UnixMilli(convRs)
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_owner_record where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_set_owner_record查询 digest+vaultId失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_owner_record digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_owner_record(vault_id,caller,previous_owner,new_owner,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_owner, new_owner, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_owner_record新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_owner_record新增id：=%v", lastInsertID)

	upVaultSql := "update vault set owner=? where vault_id=?"
	rsUp, errUp := con.Exec(upVaultSql, new_owner, vaultId)
	if errUp != nil {
		log.Printf("vault owner更新失败: %v", errUp)
		defer con.Close()
		return
	}
	updateRowCount, _ := rsUp.RowsAffected()
	log.Printf("vault owner updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func InsertVaultSetCuratorRecord(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_curator_record where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_set_curator_record查询 digest+vaultId失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_set_curator_record digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_curator_record(vault_id,caller,previous_curator,new_curator,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, vaultId, caller, previous_curator, new_curator, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_curator_record新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_curator_record新增id：=%v", lastInsertID)

	upVaultSql := "update vault set curator=? where vault_id=?"
	rsUp, errUp := con.Exec(upVaultSql, new_curator, vaultId)
	if errUp != nil {
		log.Printf("vault curator更新失败: %v", errUp)
		defer con.Close()
		return
	}
	updateRowCount, _ := rsUp.RowsAffected()
	log.Printf("vault updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func InsertWithdrawSupplyQueue(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_withdraw_queue where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_withdraw_queue查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vvault_withdraw_queue digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_withdraw_queue(vault_id,caller,queue,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, caller, vaultId, queueStr, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_allocation_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_withdraw_queue新增id：=%v", lastInsertID)
	defer con.Close()
}

func InsertVaultSupplyQueue(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)
	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_supply_queue where digest=? and vault_id=?", digest, vaultId)
	if queryErr != nil {
		log.Printf("vault_supply_queue查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("vault_supply_queue digest+vaultId exist :%v,%v\n", digest, vaultId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_supply_queue(vault_id,caller,queue,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, caller, vaultId, queueStr, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_supply_queue新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_supply_queue新增id：=%v", lastInsertID)
	defer con.Close()
}

func SetAllocationCap(parsedJson map[string]interface{}, digest string, transactionTimeUnix string) {
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
	ttConvRs, ttConvErr := strconv.ParseInt(transactionTimeUnix, 10, 64)
	if ttConvErr != nil {
		log.Printf("transactionTimeUnix转换失败：%v\n", convErr)
	}
	transactionTime := time.UnixMilli(ttConvRs)

	con := common.GetDbConnection()
	queryRs, queryErr := con.Query("select * from vault_set_allocation_cap where digest=? and market_id=?", digest, marketId)
	if queryErr != nil {
		log.Printf("vault_set_allocation_cap查询 digest失败: %v", queryErr)
		defer con.Close()
		return
	}
	if queryRs.Next() {
		fmt.Printf("SetAllocationCapEvent digest+marketId exist :%v,%v\n", digest, marketId)
		defer queryRs.Close()
		defer con.Close()
		return
	}

	sql := "insert into vault_set_allocation_cap(market_id,vault_id,cap,weight_bps,caller,timestamp_ms_unix,timestamp_ms,digest,transaction_time_unix,transaction_time) value(?,?,?,?,?,?,?,?,?,?)"
	result, err := con.Exec(sql, marketId, vaultId, cap, weightBps, caller, timestampMsUnix, timestampMs, digest, transactionTimeUnix, transactionTime)
	if err != nil {
		log.Printf("vault_set_allocation_cap新增失败: %v", err)
		defer con.Close()
		return
	}
	lastInsertID, _ := result.LastInsertId()
	log.Printf("vault_set_allocation_cap新增id：=%v", lastInsertID)
	defer con.Close()
}

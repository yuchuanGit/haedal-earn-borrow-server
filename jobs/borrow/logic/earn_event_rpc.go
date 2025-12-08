package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"haedal-earn-borrow-server/common"
	"log"
	"strings"

	"github.com/aptos-labs/aptos-go-sdk/bcs"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/mystenbcs"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
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
			case VaultEvent: //创建Vault
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
		totalAsset := "0"
		for _, returnValue := range moveCallReturn[0].ReturnValues {
			bcsBytes, _ := anyToBytes(returnValue.([]any)[0])
			deserializer := bcs.NewDeserializer(bcsBytes)
			val := deserializer.U128()
			totalAsset = val.String()
		}
		if totalAsset != "0" {
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
		log.Printf("vault_borrow_cap新增失败: %v", err)
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
	valutSharedObject, err := GetSharedObjectRef(ctx, cli, "0x53e1df5dc9a011c784106a1f8785b15048198a603ab363c14732fee3a100a42f", true)
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

type VaultModel struct {
	VaultId       string
	VaultName     string
	AssetType     string
	HtokenType    string
	AssetDecimals float64 //存入精度
	TotalShares   string  //存入总的份额
	AssetReserve  string  //总的闲置数量
	SupplyCap     string  //Vault 最大存入数量
	MaxDeposit    string  //Vault 单次最大存入数量
	MinDeposit    string  //Vault 单次最小存入数量
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
		fmt.Printf("RpcApiRequest SuiGetObject err:%v\n", err)
	}
	if resp.Data != nil {
		if resp.Data.Content != nil {
			var vm VaultModel
			vm.VaultId = vaultId
			fields := resp.Data.Content.Fields
			vm.VaultName = fields["vault_name"].(string)
			vm.AssetDecimals = fields["asset_decimals"].(float64) //存入精度
			vm.TotalShares = fields["total_shares"].(string)      //存入总的份额
			// vm.TotalShares = fmt.Sprintf("%.0f", total_shares)
			vm.AssetReserve = fields["asset_reserve"].(string) //总的闲置数量
			strategy := fields["strategy"].(map[string]interface{})
			if strategy != nil {
				strategyFields := strategy["fields"].(map[string]interface{})
				vm.SupplyCap = strategyFields["supply_cap"].(string)   //Vault 最大存入数量
				vm.MaxDeposit = strategyFields["max_deposit"].(string) //Vault 单次最大存入数量
				vm.MinDeposit = strategyFields["min_deposit"].(string) //Vault 单次最小存入数量
			}
			VaultInfoUpdate(vm)
		} else {
			fmt.Printf("RpcApiRequest SuiGetObject resp.Data.Content:%v\n", resp.Data.Content)
		}
	} else {
		fmt.Printf("RpcApiRequest SuiGetObject resp.Data:%v\n", resp.Data)
	}

	// res.Data.Content.Fields
}

func VaultInfoUpdate(vm VaultModel) {
	con := common.GetDbConnection()
	sql := "update vault set vault_name=?,asset_decimals=?,total_shares=?,asset_reserve=?,supply_cap=?,max_deposit=?,min_deposit=? where vault_id=?"
	result, err := con.Exec(sql, vm.VaultName, vm.AssetDecimals, vm.TotalShares, vm.AssetReserve, vm.SupplyCap, vm.MaxDeposit, vm.MinDeposit, vm.VaultId)
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
	if len(jobTasks) == 1 {
		fmt.Printf("ScanVaultEvent最后一条：%v\n", jobTasks[0].InputObjectId)
		RpcRequestScanVaultEvent(jobTasks[0], true)
		QueryVaultInfoUpdate(jobTasks[0].InputObjectId)
		ExecuteMoveUpdateVaultTotalAsset(jobTasks[0].InputObjectId)
	} else if len(jobTasks) > 1 {
		RpcRequestScanVaultEvent(jobTasks[0], false)
		QueryVaultInfoUpdate(jobTasks[0].InputObjectId)
		ExecuteMoveUpdateVaultTotalAsset(jobTasks[0].InputObjectId)
	}
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
			UpdateTimingTypeExecutionCompleted(false, ScheduledTaskTypeVault, 0) //更新所有Vault任务未执行
		} else {
			UpdateTimingTypeExecutionCompleted(true, ScheduledTaskTypeVault, jobInfo.Id) //更新所有Vault任务已完成执行
		}
		return
	}
	for _, data := range resp.Data {
		digest := data.Digest
		// transactionTime := data.TimestampMs
		for _, event := range data.Events {
			eventType := EventType(event.Type)
			switch eventType {
			case SetAllocationEvent: //设置cap
				SetAllocationCap(event.ParsedJson, digest)
			case SetSupplyQueueEvent: //设置存款队列，Vault和Market相关联
				InsertVaultSupplyQueue(event.ParsedJson, digest)
			case SetWithdrawQueueEvent: //设置取款队列，Vault和Market相关联
				InsertWithdrawSupplyQueue(event.ParsedJson, digest)
			case SetCuratorEvent: //Vault设置更新Curator记录
				InsertVaultSetCuratorRecord(event.ParsedJson, digest)
			case SetAllocatorEvent: //Vault设置更新Allocator记录
				InsertVaultSetAllocatorRecord(event.ParsedJson, digest)
			case SubmitTimelockEvent: //提交时间锁记录
				InsertVaultSubmitTimeLock(event.ParsedJson, digest)
			case SubmitGuardianEvent: //提交Guardian生效记录
				InsertVaultSubmitGuardian(event.ParsedJson, digest)
			case RevokePendingEvent: //Vault撤销待定记录
				InsertVaultRevokePending(event.ParsedJson, digest)
			case SubmitSupplyCapEvent: //vault提交生效cap
				InsertVaultSubmitSupplyCap(event.ParsedJson, digest)
			case ApplySupplyCapEvent: //vault应用存入上限cap
				InsertVaultApplySupplyCap(event.ParsedJson, digest)
			case SubmitMarketRemovalEvent: //vault提交移除Market
				InsertVaultSubmitMarketRemoval(event.ParsedJson, digest)
			case RemoveMarketEvent: //vault提交移除Market
				InsertVaultRemoveMarket(event.ParsedJson, digest)
			case SetMinDepositEvent: //vault设置最小押金
				InsertVaultSetMinDeposit(event.ParsedJson, digest)
			case SetMaxDepositEvent: //vault设置最大押金
				InsertVaultSetMaxDeposit(event.ParsedJson, digest)
			case SetWithdrawCooldownEvent: //vault设置提款冷却事件
				InsertVaultSetWithdrawCooldown(event.ParsedJson, digest)
			case SetMinRebalanceIntervalEvent: //设置最小再平衡间隔
				InsertVaultSetMinRebalanceInterval(event.ParsedJson, digest)
			case UpdateLastRebalanceEvent: //更新上次重新平衡事件
				InsertVaultUpdateLastRebalance(event.ParsedJson, digest)
			case SetFeeRecipientEvent: //设置费用接收人
				InsertVaultSetFeeRecipient(event.ParsedJson, digest)
			case SubmitPerformanceFeeEvent: //提交绩效费
				InsertVaultSubmitPerformanceFee(event.ParsedJson, digest)
			case ApplyPerformanceFeeEvent: //申请绩效费
				InsertVaultApplyPerformanceFee(event.ParsedJson, digest)
			case SubmitManagementFeeEvent: //提交管理费
				InsertVaultSubmitmentFee(event.ParsedJson, digest)
			case ApplyManagementFeeEvent: //申请管理费
				InsertVaultApplyManagementFee(event.ParsedJson, digest)
			case VaultDepositEvent: //用户存入Vault池
				InsertVaultDeposit(event.ParsedJson, digest)
			case VaultWithdrawEvent: //用户取出Vault池
				InsertVaultWithdraw(event.ParsedJson, digest)
			case SetVaultNameEvent: //设置Vault名称
				InsertVaultSetName(event.ParsedJson, digest)
			case CompensateLostAssetsEvent: //Vault补偿损失资产
				InsertVaultCompensateLostAssets(event.ParsedJson, digest)
			case AccrueFeesEvent: //Vault池应计费用
				InsertVaultAccrueFees(event.ParsedJson, digest)
			case RebalanceEvent: //Vault池Rebalance
				InsertVaultRebalance(event.ParsedJson, digest)
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
	sql := "SELECT id,digest,input_object_id from scheduled_task_record where timing_type=? and execution_completed=?  limit 2"
	rs, err := con.Query(sql, ScheduledTaskTypeVault, false)
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
	TimingType         string  //定时类型 1 borrow事件扫描 2.vault事件扫描'
	Digest             *string //下一次扫描事件游标
	InputObjectId      string  //扫描交易合约函数入参id
	ExecutionCompleted bool    //扫描是否完成 0 为完成 1 完成
}

func EventRpcRequest(req models.SuiXQueryTransactionBlocksRequest) models.SuiXQueryTransactionBlocksResponse {
	var response models.SuiXQueryTransactionBlocksResponse
	cli := sui.NewSuiClient(SuiEnv)
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

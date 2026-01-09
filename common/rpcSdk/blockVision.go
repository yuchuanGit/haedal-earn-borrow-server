package rpcSdk

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/signer"
	"github.com/block-vision/sui-go-sdk/sui"
)

const (
	// PackageId           = "0x98d62ae8e092da2885c486f29abaa3886fc4c4c5d6ed97f5fee9d6c62799e821" // 01-04 14:37
	// HEarnObjectId       = "0x110a702ca487e9439cee4e12224e37018baf54b7ba34c800f915eb34d836c9d0"
	// OracleObjectId      = "0x39630b105880029e71c3d4f06a62fb1327aa712c92bbc50078717cd9f618db27"
	// FarmingObjectId     = "0x9709ec470abbfc8a7c32ad68c95ee3caf0571204599bf4e997a8e56c7f7f9f11"
	// MultiplyPackageId   = "0xa6a648fc4c804ae06fbd1f27671dc3bb3716d8169934fae1d9d77db64e06ef30"
	// MultiplycfgObjectId = "0x584967a4cd28d4f3a64f7a0f99a3fdb929b467125433ef4e09b1cb8f302c2ab1"
	PackageId           = "0x81f5e356224fc096a4515481220ea8e5f14e75f27ed66c0e2c521628f0eae16c" // 01-09 18:11
	HEarnObjectId       = "0xa799df556e37185f1c9386ce81513c48d8650bc1d5a6f0802187c1e80dc0255e"
	OracleObjectId      = "0x77fa7603602f0a1c562d6511b3575367e39a40fb6e304dd29361910ad81e700c"
	FarmingObjectId     = "0x795e4ce5fd488890403a4a561f81156c71aaa4d6174a3a1b6c0ce254168e2a8c"
	MultiplyPackageId   = "0x29da558770eb5663d6069d1c79399ba754c45b395a9088a4a9a093a383fa251c"
	MultiplycfgObjectId = "0x6c05a3b26f0e4b3965526e47860b5f13e55903fa375bc5c6a4a17515a6e8b525"
	SuiUserAddress      = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
	SuiUserPrivKey      = "suiprivkey1qzd62m4ww3w7fsxdxph63xcrzfulc0s6pl0df83vlhwgp6kehtvzv4dpm54"
	SuiBlockvisionEnv   = "https://sui-testnet-endpoint.blockvision.org"
	SuiEnv              = "https://fullnode.testnet.sui.io:443"
)

func QuerySuiGetObject(objectId string, typeLog string) *models.SuiObjectData {
	cli := sui.NewSuiClient(SuiEnv)
	ctx := context.Background()
	reqParams := models.SuiGetObjectRequest{
		ObjectId: objectId,
		Options: models.SuiObjectDataOptions{
			ShowType:    true,
			ShowContent: true,
			ShowBcs:     false,
		},
	}
	resp, err := cli.SuiGetObject(ctx, reqParams)
	if err != nil {
		fmt.Printf("%v SuiGetObject err:%v\n", typeLog, err)
		return nil
	}
	return resp.Data
}
func SuiTransactionBlockInputParameter(inputObjectId string, nextCursor string) models.SuiXQueryTransactionBlocksRequest {
	params := models.SuiXQueryTransactionBlocksRequest{
		SuiTransactionBlockResponseQuery: models.SuiTransactionBlockResponseQuery{
			TransactionFilter: models.TransactionFilter{
				"InputObject": inputObjectId,
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

func EventRpcRequest(req models.SuiXQueryTransactionBlocksRequest, typeLog string) models.SuiXQueryTransactionBlocksResponse {
	var response models.SuiXQueryTransactionBlocksResponse
	cli := sui.NewSuiClient(SuiBlockvisionEnv)
	ctx := context.Background()
	resp, err := cli.SuiXQueryTransactionBlocks(ctx, req)
	if err != nil {
		fmt.Printf("RpcApiRequest %v err:%v\n", typeLog, err)
		return response
	}
	return resp
}

func ExecuteSignAndExecuteTransactionBlock(cli sui.ISuiAPI, ctx context.Context, moduleName string, funcName string, typeArguments []interface{}, arguments []interface{}) []moveCallResult {
	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          SuiUserAddress, //交易签名人的Sui地址
		PackageObjectId: PackageId,
		Module:          moduleName,
		Function:        funcName,
		TypeArguments:   typeArguments,
		Arguments:       arguments,
		GasBudget:       "10000000",
	})

	var moveCallReturn []moveCallResult
	if err != nil {
		fmt.Printf("MoveCallRequest失败：%v\n", err)
		return moveCallReturn
	}

	// signerAccount, err := signer.NewSignertWithMnemonic("mnemonic phrase")
	signerAccount, err := signer.NewSignerWithSecretKey(SuiUserPrivKey)
	devRs, devErr := cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      signerAccount.PriKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:         true,
			ShowRawInput:      true,
			ShowEffects:       true,
			ShowObjectChanges: true,
		},
		RequestType: "WaitForLocalExecution"})
	if devErr != nil {
		fmt.Printf("SignAndExecuteTransactionBlock执行失败：%v\n", devErr.Error())
		return moveCallReturn
	}
	if devRs.Effects.Status.Status == "failure" {
		fmt.Println("SignAndExecuteTransactionBlock Status 失败:", devRs.Effects.Status.Error)
		return moveCallReturn
	}
	resultsMarshalled, err2 := devRs.Results.MarshalJSON()
	if err2 != nil {
		fmt.Println("SignAndExecuteTransactionBlock MarshalJSON 失败:", err2.Error())
		return moveCallReturn
	}

	err3 := json.Unmarshal(resultsMarshalled, &moveCallReturn)
	if err3 != nil {
		fmt.Println("moveCallReturn convert json fail ", err3.Error())
		return moveCallReturn
	}
	return moveCallReturn
}

type moveCallResult struct {
	ReturnValues []interface{}
}

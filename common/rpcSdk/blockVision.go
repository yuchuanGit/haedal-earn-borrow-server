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
	PackageId           = "0xc3c159c807ecedc244cbb4c3eac20bfee994cf86df4fc0f356e24051c2211ba0" // 01-09 13:39
	HEarnObjectId       = "0xd08deab2e0cdd20dda1015ede5180fafc82f031492748bb352bf6d7b60c79e2d"
	OracleObjectId      = "0x7f644df6028a35324112f59337a3e3594f42d13f55f2c3f2da16699f4838d4e4"
	FarmingObjectId     = "0x39283e0320c8db9896274ae726f92337e872855acfc473e15c71f439b2203897"
	MultiplyPackageId   = "0x9fcc948a02bc35b1472bf21e546e48af27df35dc9538bb6d8711a71bbb18afc2"
	MultiplycfgObjectId = "0xb2b7478ead0d87a0c0894cbde75cdb8234411357009bb64e079c2f9b722be0ec"
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

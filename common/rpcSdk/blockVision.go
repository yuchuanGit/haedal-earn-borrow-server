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
	SuiUserAddress = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
	// SuiUserAddress    = "0xec731dad64e781caff49561ed2a69e873b0c1977f923786b0b803c2386dfd19a"
	SuiBlockvisionEnv = "https://sui-testnet-endpoint.blockvision.org"
	SuiEnv            = "https://fullnode.testnet.sui.io:443"
)

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
		PackageObjectId: "0x6674e6da8ca13907d0850c603ccf72a1b5a871c88cacc74716e6712ad19622fb",
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
	signerAccount, err := signer.NewSignerWithSecretKey("suiprivkey1qzd62m4ww3w7fsxdxph63xcrzfulc0s6pl0df83vlhwgp6kehtvzv4dpm54")
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

type moveCallResult struct {
	ReturnValues []interface{}
}

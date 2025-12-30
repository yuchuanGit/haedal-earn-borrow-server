package rpcSdk

import (
	"context"
	"fmt"

	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/sui"
)

const (
	SuiUserAddress    = "0x438796b44e606f8768c925534ebb87be9ded13cc51a6ddd955e6e40ab85db6f5"
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

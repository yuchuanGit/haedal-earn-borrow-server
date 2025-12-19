package common

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
)

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

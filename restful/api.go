package main

import (
	"haedal-earn-borrow-server/restful/logic"
	"log"
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	// router.Use(Recover(), gin.Logger())
	GetBorrowList(router)
	TotalCollateralBorrow(router)
	YourTotalSupplyLine(router)

	QueryBorrowDetail(router)
	QueryBorrowDetailLine(router)
	QueryBorrowDetailRateLine(router)
	QueryBorrowDetailRateModel(router)
	UserBorrowDetail(router)

	router.Run()
}

func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// 1. 记录 panic 详情（堆栈信息+请求上下文，便于排查）
				slog.Error("请求处理发生 panic",
					"err", err,
					"path", c.Request.URL.Path,
					"method", c.Request.Method,
					"headers", c.Request.Header,
					"stack", string(debug.Stack()),
				)

				// 2. 返回友好的 500 响应（避免暴露内部错误）
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"code":    500,
					"message": "服务器内部错误，请稍后重试",
					"data":    nil,
				})
			}
		}()

		// 继续执行后续中间件和处理函数
		c.Next()
	}
}

func GetBorrowList(router *gin.Engine) {
	router.GET("/haedal/GetBorrowList", func(c *gin.Context) {
		rsp, err := logic.QueryBorrowVaultList()
		if err != nil {
			log.Printf("QueryBorrowVaultList查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

type UserBorrowDetailRequest struct {
	UserAddress string `json:"userAddress"`
	MarketId    string `json:"marketId"`
}

func UserBorrowDetail(router *gin.Engine) {
	router.POST("/haedal/UserBorrowDetail", func(c *gin.Context) {
		var req UserBorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// 绑定失败（如 JSON 格式错误、字段类型不匹配）
			c.JSON(301, gin.H{
				"error": err.Error(), // 返回错误信息
			})
			return
		}
		rsp, err := logic.QueryUserBorrowDetail(req.UserAddress, req.MarketId)
		if err != nil {
			log.Printf("QueryUserBorrowDetail查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

type BorrowDetailRequest struct {
	MarketId       string `json:"marketId"`
	UserAddress    string `json:"userAddress"`
	TimePeriodType int8   `json:"timePeriodType"` //统计时间段类型 1周 2月 3年
	LineType       int8   `json:"lineType"`       // 折线图类型 1 Supply 2 Borrow
}

func QueryBorrowDetail(router *gin.Engine) {
	router.POST("/haedal/QueryBorrowDetail", func(c *gin.Context) {
		var req BorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// 绑定失败（如 JSON 格式错误、字段类型不匹配）
			c.JSON(301, gin.H{
				"error": err.Error(), // 返回错误信息
			})
			return
		}
		rsp, err := logic.QueryBorrowDetail(req.MarketId)
		if err != nil {
			log.Printf("QueryBorrowDetail查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

func QueryBorrowDetailLine(router *gin.Engine) {
	router.POST("/haedal/QueryBorrowDetailLine", func(c *gin.Context) {
		var req BorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// 绑定失败（如 JSON 格式错误、字段类型不匹配）
			c.JSON(301, gin.H{
				"error": err.Error(), // 返回错误信息
			})
			return
		}
		rsp, err := logic.QueryBorrowDetailLine(req.MarketId, req.TimePeriodType, req.LineType)
		if err != nil {
			log.Printf("QueryBorrowDetailLin查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

func YourTotalSupplyLine(router *gin.Engine) {
	router.POST("/haedal/YourTotalSupplyLine", func(c *gin.Context) {
		var req BorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// 绑定失败（如 JSON 格式错误、字段类型不匹配）
			c.JSON(301, gin.H{
				"error": err.Error(), // 返回错误信息
			})
			return
		}
		rsp, err := logic.YourTotalSupplyLine(req.UserAddress, req.TimePeriodType)
		if err != nil {
			log.Printf("YourTotalSupplyLine查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

func TotalCollateralBorrow(router *gin.Engine) {
	router.POST("/haedal/TotalCollateralBorrow", func(c *gin.Context) {
		var req BorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(301, gin.H{"error": err.Error()}) // 返回错误信息

			return
		}
		rsp, err := logic.TotalCollateralBorrow(req.UserAddress)
		if err != nil {
			log.Printf("TotalCollateralBorrow查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

func QueryBorrowDetailRateLine(router *gin.Engine) {
	router.POST("/haedal/QueryBorrowDetailRateLine", func(c *gin.Context) {
		var req BorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// 绑定失败（如 JSON 格式错误、字段类型不匹配）
			c.JSON(301, gin.H{
				"error": err.Error(), // 返回错误信息
			})
			return
		}
		rsp, err := logic.QueryBorrowDetailRateLine(req.MarketId, req.TimePeriodType, req.LineType)
		if err != nil {
			log.Printf("QueryBorrowDetailLin查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

func QueryBorrowDetailRateModel(router *gin.Engine) {
	router.POST("/haedal/QueryBorrowDetailRateModel", func(c *gin.Context) {
		var req BorrowDetailRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// 绑定失败（如 JSON 格式错误、字段类型不匹配）
			c.JSON(301, gin.H{
				"error": err.Error(), // 返回错误信息
			})
			return
		}
		rsp, err := logic.QueryBorrowDetailRateModel(req.MarketId)
		if err != nil {
			log.Printf("QueryBorrowDetailRateModel查询失败: %v", err)
		}
		c.JSON(200, gin.H{
			"data": rsp,
			"code": 200,
			"msg":  "操作成功",
		})
	})
}

package main

import (
	"fmt"
	"time"

	"haedal-earn-borrow-server/jobs/borrow/logic"

	"github.com/robfig/cron/v3"
)

func main() {
	startEventJob()
}

func startEventJob() {
	// 创建一个支持秒级的 cron 实例（默认不支持秒，需加 WithSeconds() 选项）
	c := cron.New(cron.WithSeconds())

	// 添加定时任务（每 10 秒执行一次）
	_, err := c.AddFunc("*/2 * * * * *", func() {
		fmt.Println("event cron 任务执行：", time.Now().Format("15:04:05.000"))
		nextCursor := logic.QueryEventsCursor()
		logic.RpcApiRequest(nextCursor)
		logic.RpcRequestScanCreateVault()
		logic.ScanVaultEvent()
	})
	if err != nil {
		fmt.Println("event添加任务失败：", err)
		return
	}
	// 启动定时任务（非阻塞，会在后台运行）
	c.Start()

	fmt.Println("event-job start success...")
	defer c.Stop() // 退出前停止

	select {} // 阻塞主程序（避免退出）
}

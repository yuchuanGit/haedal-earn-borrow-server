package main

import (
	"fmt"
	statistics "haedal-earn-borrow-server/jobs/statistics/logic"
	"time"

	"github.com/robfig/cron/v3"
)

func main() {
	startJob()
}

func startJob() {
	c := cron.New(cron.WithSeconds())
	// 添加定时任务（每1分钟秒执行一次）
	_, err := c.AddFunc("0 */1 * * * *", func() {
		fmt.Println("per-minute cron 任务执行：", time.Now().Format("15:04:05.000"))
		statistics.EarnTimedCollection()
		statistics.MarketTimeCollection()
	})
	if err != nil {
		fmt.Println("event添加任务失败：", err)
		return
	}
	c.Start()

	fmt.Println("per-minute start success...")
	defer c.Stop() // 退出前停止

	select {} // 阻塞主程序（避免退出）
}

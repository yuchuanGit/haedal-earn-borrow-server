package main

import (
	"fmt"
	"haedal-earn-borrow-server/jobs/borrow/logic"
	"time"

	"github.com/robfig/cron/v3"
)

func main() {
	startJob()
}

func startJob() {
	c := cron.New(cron.WithSeconds())
	// 添加定时任务（每 1小时秒执行一次）
	_, err := c.AddFunc("0 */2 * * * *", func() {
		fmt.Println("collect-every-hour cron 任务执行：", time.Now().Format("15:04:05.000"))
		logic.VaultAllLoopExecuteMove()
	})
	if err != nil {
		fmt.Println("event添加任务失败：", err)
		return
	}
	c.Start()

	fmt.Println("collect-every-hour start success...")
	defer c.Stop() // 退出前停止

	select {} // 阻塞主程序（避免退出）
}

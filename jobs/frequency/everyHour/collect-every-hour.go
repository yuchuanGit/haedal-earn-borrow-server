package main

import (
	"fmt"
	"time"

	"haedal-earn-borrow-server/jobs/borrow/logic"

	"github.com/robfig/cron/v3"
)

func main() {
	startJob()
}

func startJob() {
	c := cron.New(cron.WithSeconds())
	// 添加定时任务（每 1小时秒执行一次）
	_, err := c.AddFunc("0 0 */1 * * *", func() {
		fmt.Println("collect-every-hour cron 任务执行：", time.Now().Format("15:04:05.000"))
		logic.VaultAllLoopExecuteMove()
		// logic.VaultReallocate()
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

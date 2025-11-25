package main

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	// 1. 定义编译参数（固定 Ubuntu x86-64 配置）
	env := []string{
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH=amd64",
	}
	outputPath := "haedal-app"     // 输出文件名（Ubuntu 可执行文件，无后缀）
	mainPackagePath := "./restful" // main 包路径（你的程序入口）
	ldFlags := "-w -s"             // 编译优化参数

	// 2. 构造 go build 命令
	cmdArgs := []string{
		"build",
		"-ldflags", ldFlags,
		"-o", outputPath,
		mainPackagePath,
	}

	// 3. 获取项目根目录（确保命令在项目根目录执行）
	rootDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("获取项目目录失败：%v", err)
	}

	// 4. 执行编译命令（内置环境变量，不受系统影响）
	cmd := exec.Command("go", cmdArgs...)
	cmd.Dir = rootDir                      // 执行目录：项目根目录
	cmd.Env = append(os.Environ(), env...) // 合并系统环境变量 + 自定义编译环境变量
	cmd.Stdout = os.Stdout                 // 输出编译日志到终端
	cmd.Stderr = os.Stderr                 // 输出错误日志到终端

	// 5. 启动编译并等待完成
	log.Println("开始编译 Ubuntu x86-64 版本...")
	log.Printf("执行命令：go %v", cmdArgs)
	if err := cmd.Run(); err != nil {
		log.Fatalf("编译失败：%v", err)
	}

	log.Printf("编译成功！输出文件：%s", filepath.Join(rootDir, outputPath))
}

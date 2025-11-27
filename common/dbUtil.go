package common

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL 驱动（下划线表示只引入不直接使用）
	"gopkg.in/ini.v1"
)

func GetDbConnection() *sql.DB {
	if err := InitConfig(); err != nil {
		log.Fatalf("配置初始化失败：%v", err)
	}
	mysqlConf := GlobalConfig.MySQL
	var builder strings.Builder
	builder.WriteString(mysqlConf.UserName)
	builder.WriteString(":")
	builder.WriteString(mysqlConf.Password)
	builder.WriteString("@tcp(")
	builder.WriteString(mysqlConf.Host + ":" + mysqlConf.Port)
	builder.WriteString(")/" + mysqlConf.DBName)
	builder.WriteString("?charset=utf8mb4&parseTime=True&loc=Local")
	dsn := builder.String()
	// log.Printf("dsn=%v\n", dsn)
	// dsn := "haedalOne:haedalOne!@#@tcp(52.74.206.162:3307)/haedal_earn_borrow?charset=utf8mb4&parseTime=True&loc=Local"

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("连接数据库失败: %v\n", err)
	}
	//defer db.Close() // 程序退出时关闭数据库连接

	// 验证连接是否有效
	err = db.Ping()
	if err != nil {
		log.Printf("数据库连接无效: %v\n", err)
	}
	fmt.Println("数据库连接成功！")
	return db
}

var GlobalConfig = struct {
	MySQL MySQLConfig `ini:"prdMysql"` // 对应 ini 中的 [proMysql] 配置组
}{}

type MySQLConfig struct {
	Host         string `ini:"host"` // 对应 ini 中的 host
	Port         string `ini:"port"` // 对应 ini 中的 port（自动转换为 int 类型）
	UserName     string `ini:"user_name"`
	Password     string `ini:"password"`
	DBName       string `ini:"db_name"` // 结构体字段名与 ini 键名不一致时，通过 tag 指定
	Charset      string `ini:"charset"`
	MaxOpenConns int    `ini:"max_open_conns"`
	MaxIdleConns int    `ini:"max_idle_conns"`
	ConnMaxLife  int    `ini:"conn_max_lifetime"`
}

// GetCurrentSourceDir 获取当前源代码文件所在目录（仅开发go run 环境有效）
func GetCurrentSourceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

func InitConfig() error {
	// 1. 获取配置文件绝对路径（避免相对路径问题）
	// 项目根目录/conf/app.ini
	confPath := filepath.Join(GetProjectRoot(), "conf", "app.ini") //pro环境
	// confPath := filepath.Join(GetCurrentSourceDir()+"/../", "conf", "app.ini") //dev环境
	fmt.Printf("加载配置文件：%s\n", confPath)

	// 2. 检查配置文件是否存在
	if _, err := os.Stat(confPath); os.IsNotExist(err) {
		return fmt.Errorf("配置文件不存在：%s", confPath)
	}
	//关键配置：强制保留双引号内的原始值，禁用所有特殊解析
	loadOpts := ini.LoadOptions{
		//指示是否忽略值末尾的注释并将其视为值的一部分。
		IgnoreInlineComment: false,
		// 自动去除双引号（无需手动处理，值更纯净）
		UnescapeValueDoubleQuotes: true,
	}
	// 3. 加载并解析 ini 文件
	// cfg, err := ini.Load(confPath)
	cfg, err := ini.LoadSources(loadOpts, confPath)
	if err != nil {
		return fmt.Errorf("解析配置文件失败：%w", err)
	}

	// 4. 将配置映射到结构体（自动匹配字段与配置项）
	if err := cfg.MapTo(&GlobalConfig); err != nil {
		return fmt.Errorf("配置映射结构体失败：%w", err)
	}

	return nil
}

// GetProjectRoot 获取项目根目录（关键：解决不同目录执行的相对路径问题）
func GetProjectRoot() string {
	// 获取当前可执行文件路径
	exePath, err := os.Executable()
	if err != nil {
		log.Printf("获取执行路径失败：%v", err)
		return ""
	}
	log.Printf("Executable=%v", exePath)
	// 向上级目录回溯，直到找到项目根目录（假设 conf 目录在根目录下）
	rootPath := filepath.Dir(exePath)
	for !isProjectRoot(rootPath) {
		rootPath = filepath.Dir(rootPath)
		// 防止无限循环（到达系统根目录）
		if rootPath == "/" || rootPath == "\\" {
			log.Printf("未找到项目根目录（conf 目录不存在）")
			return ""
		}
	}
	return rootPath
}

// isProjectRoot 判断是否为项目根目录（存在 conf 目录则认为是根目录）
func isProjectRoot(path string) bool {
	confDir := filepath.Join(path, "conf")
	_, err := os.Stat(confDir)
	return !os.IsNotExist(err)
}

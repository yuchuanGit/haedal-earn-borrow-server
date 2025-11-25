package common

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" // MySQL 驱动（下划线表示只引入不直接使用）
)

func GetDbConnection() *sql.DB {
	dsn := "root:123456@tcp(127.0.0.1:3306)/haedal_earn_borrow?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	//defer db.Close() // 程序退出时关闭数据库连接

	// 验证连接是否有效
	err = db.Ping()
	if err != nil {
		log.Fatalf("数据库连接无效: %v", err)
	}
	fmt.Println("数据库连接成功！")
	return db
}

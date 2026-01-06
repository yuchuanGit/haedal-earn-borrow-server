package common

import (
	"database/sql"
	"fmt"
	"haedal-earn-borrow-server/common/mydb"
	"log"
)

const (
	ScheduledTaskTypeBorrow        = 1
	ScheduledTaskTypeVault         = 2
	ScheduledTaskTypeFarming       = 3
	ScheduledTaskTypeFarmingPoolId = 31
	ScheduledTaskTypeMultiply      = 4
)

func InsertScheduledTask(con *sql.DB, timingType int, inputObjectId string, operationType string) {
	isInsertTask := true
	log.Printf("isInsertTask：=%v", isInsertTask)
	taskRs, taskErr := con.Query("select * from scheduled_task_record where timing_type=? and input_object_id=?", timingType, inputObjectId)
	if taskErr != nil {
		log.Printf("%v scheduled_task_record查询 input_object_id失败: %v", operationType, taskErr)
		defer con.Close()
		return
	}
	if taskRs.Next() {
		isInsertTask = false
	}
	if isInsertTask {
		sqlTask := "insert into scheduled_task_record(timing_type,input_object_id,execution_completed) value(?,?,?)"
		resultTask, errTask := con.Exec(sqlTask, timingType, inputObjectId, 0)
		if errTask != nil {
			log.Printf("%v  scheduled_task_record 新增失败: %v", operationType, errTask)
			defer con.Close()
			return
		}
		taskLastInsertID, _ := resultTask.LastInsertId()
		log.Printf("%v scheduled_task_record新增id：=%v", operationType, taskLastInsertID)
	}
}

func UpdateTimingTypeExecutionCompleted(executionCompleted bool, timingType int, id int) {
	con := mydb.GetDbConnection()
	sql := "update scheduled_task_record set execution_completed=? where timing_type=?"
	var params []any
	params = append(params, executionCompleted)
	params = append(params, timingType)
	if executionCompleted {
		sql = sql + " and id=?"
		params = append(params, id)
	}
	_, upErr := con.Exec(sql, params...)
	if upErr != nil {
		log.Printf("UpdateTimingTypeExecutionCompleted scheduled_task_record execution_completed=0失败：%v\n", upErr.Error())
	}
	defer con.Close()
}

func EventsCursorUpdateById(digest string, id int) {
	if digest == "" || digest == "null" || digest == "undefined" {
		return
	}
	con := mydb.GetDbConnection()
	sql := "update scheduled_task_record set digest=? where id=?"
	rs, err := con.Exec(sql, digest, id)
	if err != nil {
		log.Printf("scheduled_task_record update digest失败：%v\n", err)
	}
	updateRowCount, _ := rs.RowsAffected()
	log.Printf("scheduled_task_record updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func EventsCursorUpdate(digest string, timingType int) {
	con := mydb.GetDbConnection()
	sql := "update scheduled_task_record set digest=? where timing_type=?"
	rs, err := con.Exec(sql, digest, timingType)
	if err != nil {
		log.Printf("scheduled_task_record update digest失败：%v\n", err)
	}
	updateRowCount, _ := rs.RowsAffected()
	log.Printf("scheduled_task_record updateRowCount=:%d\n", updateRowCount)
	defer con.Close()
}

func QueryEventsCursor(timingType int) string {
	// digest := ""
	var digest sql.NullString
	con := mydb.GetDbConnection()
	sql := "select digest from scheduled_task_record where timing_type=?"
	err := con.QueryRow(sql, timingType).Scan(&digest)
	if err != nil {
		log.Printf("QueryEventsCursor失败：%v\n", err)
		defer con.Close()
		return ""
	}
	defer con.Close()
	if digest.Valid {
		return digest.String
	}
	return ""
}

func QueryExecutionInputObjectId(timingType int, operationType string) []ScheduledTaskRecord {
	var jobTasks []ScheduledTaskRecord
	con := mydb.GetDbConnection()
	// sql := "SELECT id,digest,input_object_id from scheduled_task_record where timing_type=? and execution_completed=?"
	// rs, err := con.Query(sql, ScheduledTaskTypeVault, false)
	sql := "SELECT id,digest,input_object_id from scheduled_task_record where timing_type=?"
	rs, err := con.Query(sql, timingType)
	if err != nil {
		fmt.Printf("QueryExecutionInputObjectId %v 查询borrow失败：%v\n", operationType, err.Error())
		defer con.Close()
		return jobTasks
	}
	for rs.Next() {
		var tr ScheduledTaskRecord
		rs.Scan(&tr.Id, &tr.Digest, &tr.InputObjectId)
		jobTasks = append(jobTasks, tr)
	}
	defer con.Close()
	return jobTasks
}

type ScheduledTaskRecord struct {
	Id                 int
	TimingType         string  // 定时类型 1 borrow事件扫描 2.vault事件扫描'
	Digest             *string // 下一次扫描事件游标
	InputObjectId      string  // 扫描交易合约函数入参id
	ExecutionCompleted bool    // 扫描是否完成 0 为完成 1 完成
}

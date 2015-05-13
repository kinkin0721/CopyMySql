// CopyMySql project main.go
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
)

func checkError(err error) {
	if err != nil {
		//fmt.Println(err)
		panic(err.Error())
	}
}

func main() {
	sql_ip_port := "172.26.48.2:3306"
	sql_user := "root"
	sql_pwd := "root"

	sql_base_src := "go_test"
	sql_base_dst := "go_test_copy"
	sql_table := "test_table"
	sql_id := "1,2,3,4"

	arg_num := len(os.Args)

	if arg_num > 1 {
		sql_base_src = os.Args[1]
	}
	if arg_num > 2 {
		sql_base_dst = os.Args[2]
	}
	if arg_num > 3 {
		sql_table = os.Args[3]
	}
	if arg_num > 4 {
		sql_id = os.Args[4]
	}

	db, err := sql.Open("mysql", sql_user+":"+sql_pwd+"@tcp("+sql_ip_port+")/js_base")
	checkError(err)
	defer db.Close()

	rows, err := db.Query("select column_name from information_schema.columns where table_schema = '" + sql_base_src + "' and table_name = '" + sql_table + "' and column_key = 'pri';")
	var primary_key string
	for rows.Next() {
		err := rows.Scan(&primary_key)
		checkError(err)
	}

	ids := strings.Split(sql_id, ",")

	str_ids := ""
	if sql_id != "" {
		for i := 0; i < len(ids); i++ {
			if i == 0 {
				str_ids += " where " + primary_key + " = " + ids[i]
			} else {
				str_ids += " or " + primary_key + " = " + ids[i]
			}
		}
	}

	rows_src, err := db.Query("select * from " + sql_base_src + "." + sql_table + str_ids + ";")
	checkError(err)
	defer rows_src.Close()
	_, err = db.Exec("delete from " + sql_base_dst + "." + sql_table + str_ids + ";")
	checkError(err)

	colNames_src, err := rows_src.Columns()
	readCols := make([]interface{}, len(colNames_src))
	writeCols := make([]string, len(colNames_src))
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}

	rows_dst, err := db.Query("select * from " + sql_base_dst + "." + sql_table + ";")
	checkError(err)
	defer rows_dst.Close()
	colNames_dst, err := rows_dst.Columns()

	colNames := ""
	col_params := ""
	for i, _ := range colNames_dst {
		if i == 0 {
			colNames += colNames_dst[i]
			col_params += "?"
		} else {
			colNames += ", " + colNames_dst[i]
			col_params += ", ?"
		}
	}

	stmt, err := db.Prepare("INSERT INTO " + sql_base_dst + "." + sql_table + " ( " + colNames + " ) VALUES ( " + col_params + " );")
	checkError(err)
	defer stmt.Close()

	tx, err := db.Begin()
	checkError(err)

	for rows_src.Next() {
		err = rows_src.Scan(readCols...)
		checkError(err)

		cols := make([]interface{}, len(colNames_dst))
		j := 0
		for i, _ := range colNames_src {
			if colNames_dst[j] == colNames_src[i] {
				cols[j] = readCols[i]
				j += 1

				if j >= len(colNames_dst) {
					break
				}
			}
		}

		_, err := tx.Stmt(stmt).Exec(cols...)
		checkError(err)
	}

	tx.Commit()
	fmt.Print("上手に焼けました")
}
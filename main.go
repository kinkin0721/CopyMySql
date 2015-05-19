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

var sql_ip_port string
var sql_user string
var sql_pwd string

var sql_base_src string
var sql_base_dst string
var sql_tables_ids string

func setArg() {

	sql_ip_port = ""
	sql_user = ""
	sql_pwd = ""

	sql_base_src = ""
	sql_base_dst = ""
	sql_tables_ids = ""

	arg_num := len(os.Args)

	if arg_num > 1 {
		sql_ip_port = os.Args[1]
	} else {
		fmt.Print("missing sql_ip_port\n")
	}
	if arg_num > 2 {
		sql_user = os.Args[2]
	} else {
		fmt.Print("missing sql_user\n")
	}
	if arg_num > 3 {
		sql_pwd = os.Args[3]
	} else {
		fmt.Print("missing sql_pwd\n")
	}
	if arg_num > 4 {
		sql_base_src = os.Args[4]
	} else {
		fmt.Print("missing sql_base_src\n")
	}
	if arg_num > 5 {
		sql_base_dst = os.Args[5]
	} else {
		fmt.Print("missing sql_base_dst\n")
	}
	if arg_num > 6 {
		sql_tables_ids = os.Args[6]
	} else {
		fmt.Print("missing sql_table_id\n")
	}
}

func copyMySql(db *sql.DB, tx *sql.Tx, sql_table_ids string) {

	str_table_ids := strings.Split(sql_table_ids, ":")
	sql_table := str_table_ids[0]
	sql_id := ""
	if len(str_table_ids) > 1 {
		sql_id = str_table_ids[1]
	}

	var primary_key string
	err := db.QueryRow("select column_name from information_schema.columns where " +
		"table_schema = '" + sql_base_src + "' and table_name = '" + sql_table + "' and column_key = 'pri';").Scan(&primary_key)
	checkError(err)

	ids := strings.Split(sql_id, ",")
	str_ids := ""
	if sql_id != "" {
		for i := 0; i < len(ids); i++ {
			if i == 0 {
				str_ids += " where `" + primary_key + "` = " + ids[i]
			} else {
				str_ids += " or `" + primary_key + "` = " + ids[i]
			}
		}
	}

	rows_src, err := db.Query("select * from " + sql_base_src + "." + sql_table + str_ids + ";")
	checkError(err)
	defer rows_src.Close()

	rows_dst, err := db.Query("select * from " + sql_base_dst + "." + sql_table + ";")
	checkError(err)
	defer rows_dst.Close()

	colNames_src, err := rows_src.Columns()
	checkError(err)
	colNames_dst, err := rows_dst.Columns()
	checkError(err)

	if len(colNames_dst) != len(colNames_src) {
		fmt.Printf("column数不一致："+sql_base_dst+"."+sql_table+"的column数为%d"+"\n"+
			sql_base_src+"."+sql_table+"的column数为%d"+"\n", len(colNames_dst), len(colNames_src))
	}

	if str_ids != "" {
		_, err = db.Exec("delete from " + sql_base_dst + "." + sql_table + str_ids + ";")
		checkError(err)
	}

	col_names := ""
	col_params := ""
	colcount := 0
	for i, _ := range colNames_dst {
		for j, _ := range colNames_src {
			if colNames_dst[i] == colNames_src[j] {
				if colcount == 0 {
					col_names += "`" + colNames_dst[i] + "`"
					col_params += "?"
				} else {
					col_names += ", `" + colNames_dst[i] + "`"
					col_params += ", ?"
				}

				colcount++
			}
		}
	}

	stmt, err := db.Prepare("INSERT INTO " + sql_base_dst + "." + sql_table + " ( " + col_names + " ) VALUES ( " + col_params + " );")
	if err != nil {
		fmt.Print("INSERT INTO " + sql_base_dst + "." + sql_table + " ( " + col_names + " ) VALUES ( " + col_params + " );")
		fmt.Print(sql_table + " " + err.Error() + "\n")
	}
	defer stmt.Close()

	readCols_dst := make([]interface{}, len(colNames_dst))
	writeCols_dst := make([]string, len(colNames_dst))
	for i, _ := range writeCols_dst {
		readCols_dst[i] = &writeCols_dst[i]
	}
	mapCols_dst := make(map[string]string)
	for rows_dst.Next() {
		err = rows_dst.Scan(readCols_dst...)
		checkError(err)

		mapCols_dst[writeCols_dst[0]] = ""
	}

	readCols_src := make([]interface{}, len(colNames_src))
	writeCols_src := make([]string, len(colNames_src))
	for i, _ := range writeCols_src {
		readCols_src[i] = &writeCols_src[i]
	}

	for rows_src.Next() {
		err = rows_src.Scan(readCols_src...)
		checkError(err)

		if _, ok := mapCols_dst[writeCols_src[0]]; !ok {
			cols := make([]interface{}, colcount)
			idx := 0
			for i, _ := range colNames_dst {
				for j, _ := range colNames_src {
					if colNames_dst[i] == colNames_src[j] {
						cols[idx] = readCols_src[j]
						idx++
					}
				}
			}

			_, err := tx.Stmt(stmt).Exec(cols...)
			if err != nil {
				fmt.Printf(err.Error() + " " + ",tablename=" + sql_table + ",id=" + writeCols_src[0] + "\n")
				break
			}
		}
	}
}

func main() {

	setArg()

	db, err := sql.Open("mysql", sql_user+":"+sql_pwd+"@tcp("+sql_ip_port+")/js_base")
	checkError(err)
	defer db.Close()

	tx, err := db.Begin()
	checkError(err)

	str_tables_ids := strings.Split(sql_tables_ids, ";")

	for i, _ := range str_tables_ids {
		copyMySql(db, tx, str_tables_ids[i])
	}

	tx.Commit()
	fmt.Print("上手に焼けました")
}

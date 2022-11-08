package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/xooooooox/hey"
)

type Table struct {
	TableSchema  *string   `json:"table_schema" db:"table_schema"`   // 数据库名
	TableName    *string   `json:"table_name" db:"table_name"`       // 表名
	TableComment *string   `json:"table_comment" db:"table_comment"` // 表注释
	Column       []*Column `json:"column" db:"-"`                    // 表中的所有字段
}

type Column struct {
	TableSchema            *string `json:"table_schema" db:"table_schema"`                         // 数据库名
	TableName              *string `json:"table_name" db:"table_name"`                             // 表名
	ColumnName             *string `json:"column_name" db:"column_name"`                           // 列名
	OrdinalPosition        *int    `json:"ordinal_position" db:"ordinal_position"`                 // 列序号
	ColumnDefault          *string `json:"column_default" db:"column_default"`                     // 列默认值
	IsNullable             *string `json:"is_nullable" db:"is_nullable"`                           // 是否允许列值为null
	DataType               *string `json:"data_type" db:"data_type"`                               // 列数据类型
	CharacterMaximumLength *int    `json:"character_maximum_length" db:"character_maximum_length"` // 字符串最大长度
	CharacterOctetLength   *int    `json:"character_octet_length" db:"character_octet_length"`     // 文本字符串字节最大长度
	NumericPrecision       *int    `json:"numeric_precision" db:"numeric_precision"`               // 整数最长长度|小数(整数+小数)合计长度
	NumericScale           *int    `json:"numeric_scale" db:"numeric_scale"`                       // 小数精度长度
	CharacterSetName       *string `json:"character_set_name" db:"character_set_name"`             // 字符集名称
	CollationName          *string `json:"collation_name" db:"collation_name"`                     // 校对集名称
	ColumnComment          *string `json:"column_comment" db:"column_comment"`                     // 列注释
}

var pool *sql.DB

const (
	DriverName = "postgres"
)

// DataSourceName "host=%s port=%d dbname=%s user=%s password=%s sslmode=%s"
// DataSourceName "postgres://username:password@host:port/database_name?sslmode=disable"
var DataSourceName = flag.String("d", "postgres://postgres:postgres@127.0.0.1:5432/test?sslmode=disable", "postgresql data source name")

var TableSchema = flag.String("s", "public", "postgresql database schema, multiple use \",\" separated")

var PackageName = flag.String("p", "model", "output .go source package name")

var OutputFilePath = flag.String("o", "tables.go", "output .go path")

func Init() {
	if !flag.Parsed() {
		flag.Parse()
	}
	at := time.Now()
	fmt.Printf("\u001B[1;32;48m%s <=> sql.Open(\"%s\", \"%s\")\u001B[0m\n", at.Format(time.RFC3339), DriverName, *DataSourceName)
	opened, err := sql.Open(DriverName, *DataSourceName)
	if err != nil {
		fmt.Printf("\u001B[7;31;40m%s <=> 数据库连接失败:%s\u001B[0m\n", at.Format("2006-01-02 15:04:05"), err.Error())
		panic(err)
	} else {
		opened.SetMaxOpenConns(8)
		opened.SetMaxIdleConns(8)
		opened.SetConnMaxIdleTime(time.Minute * 3)
		opened.SetConnMaxLifetime(time.Minute * 3)
		pool = opened
	}
}

func main() {
	Init()
	if err := Write(); err != nil {
		panic(err)
	}
}

func Write() error {
	tables, err := AllTable(strings.Split(*TableSchema, ","))
	if err != nil {
		return err
	}
	err = NewWriteSource(tables, *PackageName, *OutputFilePath).WriteAll()
	return err
}

func postgres() *hey.Way {
	way := hey.NewWay(pool)
	way.Prepare(hey.PreparePostgresql)
	return way
}

func PascalToUnderline(s string) string {
	var tmp []byte
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			tmp = append(tmp, '_')
		}
		if d != '_' {
			j = true
		}
		tmp = append(tmp, d)
	}
	return strings.ToLower(string(tmp[:]))
}

func UnderlineToPascal(s string) string {
	var tmp []byte
	bytes := []byte(s)
	length := len(bytes)
	nextLetterNeedToUpper := true
	for i := 0; i < length; i++ {
		if bytes[i] == '_' {
			nextLetterNeedToUpper = true
			continue
		}
		if nextLetterNeedToUpper && bytes[i] >= 'a' && bytes[i] <= 'z' {
			tmp = append(tmp, bytes[i]-32)
		} else {
			tmp = append(tmp, bytes[i])
		}
		nextLetterNeedToUpper = false
	}
	return string(tmp[:])
}

func FmtGoFile(file string) error {
	cmd := exec.Command("go", "fmt", file)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func AllTable(tableSchema []string) (result []*Table, err error) {
	result = make([]*Table, 0)
	length := len(tableSchema)
	if length == 0 {
		return
	}
	ts := fmt.Sprintf("( '%s' )", strings.Join(tableSchema, "', '"))
	prepare := "SELECT table_schema, table_name FROM information_schema.tables WHERE ( table_schema IN %s AND table_type = 'BASE TABLE' ) ORDER BY table_name ASC;"
	prepare = fmt.Sprintf(prepare, ts)
	err = postgres().Query(func(rows *sql.Rows) (err error) {
		_, err = hey.RowsAssoc(rows, &result)
		return
	}, prepare)
	for k, v := range result {
		if v.TableName == nil || *v.TableName == "" {
			continue
		}
		// 查询表注释
		// SELECT pg_tables.schemaname AS schema_name, pg_tables.tablename AS table_name, cast(obj_description(relfilenode, 'pg_class') AS VARCHAR) AS table_comment FROM pg_tables LEFT OUTER JOIN pg_class ON pg_tables.tablename = pg_class.relname WHERE ( pg_tables.schemaname = 'public' ) ORDER BY pg_tables.schemaname ASC;
		prepare = "SELECT cast(obj_description(relfilenode, 'pg_class') AS VARCHAR) AS table_comment FROM pg_tables LEFT OUTER JOIN pg_class ON pg_tables.tablename = pg_class.relname WHERE ( pg_tables.schemaname IN %s AND pg_tables.tablename = ? ) ORDER BY pg_tables.schemaname ASC LIMIT 1;"
		prepare = fmt.Sprintf(prepare, ts)
		err = postgres().Query(func(rows *sql.Rows) (err error) {
			if rows.Next() {
				tmp := ""
				result[k].TableComment = &tmp
				if err = rows.Scan(&result[k].TableComment); err != nil {
					return
				}
			}
			return
		}, prepare, *result[k].TableName)
		if err != nil {
			return
		}
		result[k].Column, err = AllColumn(tableSchema, *v.TableName)
		if err != nil {
			return
		}
	}
	return
}

func AllColumn(tableSchema []string, table string) (result []*Column, err error) {
	result = make([]*Column, 0)
	ts := fmt.Sprintf("( '%s' )", strings.Join(tableSchema, "', '"))
	prepare := "SELECT table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, character_set_name, collation_name FROM information_schema.columns WHERE ( table_schema IN %s AND table_name = ? );"
	prepare = fmt.Sprintf(prepare, ts)
	err = postgres().Query(func(rows *sql.Rows) (err error) {
		_, err = hey.RowsAssoc(rows, &result)
		return
	}, prepare, table)
	for k, v := range result {
		if v.ColumnName == nil || *v.ColumnName == "" {
			continue
		}
		// 查询列注释
		// SELECT a.attnum AS id, a.attname AS column_name, t.typname AS type_basic, SUBSTRING(FORMAT_TYPE(a.atttypid, a.atttypmod) FROM '(.*)') AS type_sql, a.attnotnull AS not_null, d.description AS comment FROM pg_class c, pg_attribute a, pg_type t, pg_description d WHERE ( c.relname = 'TABLE_NAME' AND a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND d.objoid = a.attrelid AND d.objsubid = a.attnum ) ORDER BY id ASC;
		err = postgres().Query(func(rows *sql.Rows) (err error) {
			if rows.Next() {
				tmp := ""
				result[k].ColumnComment = &tmp
				if err = rows.Scan(&result[k].ColumnComment); err != nil {
					return
				}
			}
			return
		}, "SELECT d.description AS column_comment FROM pg_class c, pg_attribute a, pg_type t, pg_description d WHERE ( c.relname = ? AND a.attname = ? AND a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid AND d.objoid = a.attrelid AND d.objsubid = a.attnum ) ORDER BY a.attnum ASC LIMIT 1;\n", table, *result[k].ColumnName)
		if err != nil {
			return
		}
	}
	return
}

func (t *Table) Comment(clean bool) (result string) {
	if t.TableComment == nil || *t.TableComment == "" {
		return
	}
	if clean {
		return *t.TableComment
	}
	return fmt.Sprintf(" // %s", *t.TableComment)
}

func (c *Column) DatabaseTypeToGoType() (types string) {
	nullable := true
	if c.IsNullable != nil && strings.ToLower(*c.IsNullable) == "no" {
		nullable = false
	}
	datatype := ""
	if c.DataType != nil {
		datatype = strings.ToLower(*c.DataType)
	}
	switch datatype {
	case "smallint", "smallserial":
		types = "int16"
	case "integer", "serial":
		types = "int"
	case "bigint", "bigserial":
		types = "int64"
	case "decimal", "numeric", "real", "double precision", "double":
		types = "float64"
	case "char", "character", "character varying", "text", "varchar", "enum":
		types = "string"
	case "bool", "boolean":
		types = "bool"
	default:
		types = "string"
	}
	if nullable && types != "" {
		types = "*" + types
	}
	return
}

func (c *Column) PostgresTypeToSetGoDefaultValue() (val string) {
	val = "\"\""
	if c.ColumnDefault == nil {
		val = "nil"
		return
	}
	types := c.DatabaseTypeToGoType()
	if strings.Contains(types, "int") || strings.Contains(types, "float") {
		if matched, err := regexp.MatchString("\\d+", *c.ColumnDefault); err == nil && matched {
			if strings.Contains(*c.ColumnDefault, "nextval(") {
				val = "0"
				return
			}
			val = fmt.Sprintf("%v", *c.ColumnDefault)
			return
		}
	}
	if strings.Contains(*c.ColumnDefault, "nextval(") {
		val = "0"
		return
	}
	if strings.Contains(*c.ColumnDefault, "char") || strings.Contains(*c.ColumnDefault, "text") {
		// reg := regexp.MustCompile("'(.*)'::.*")
		val = "\"\""
		return
	}
	val = strings.ToLower(fmt.Sprintf("%v", *c.ColumnDefault))
	if strings.ToLower(val) == "null" {
		val = "nil"
		return
	}
	return
}

func (c *Column) Comment() (result string) {
	if c.ColumnComment == nil || *c.ColumnComment == "" {
		return
	}
	return fmt.Sprintf(" // %s", *c.ColumnComment)
}

func TabName(name string) string {
	return fmt.Sprintf("Tab%s", name)
}

type WriteSourceCode struct {
	Tables         []*Table      // 数据库所有表
	PackageName    string        // 写入源文件的包名
	SourceCode     *bytes.Buffer // 源码 buffer
	OutputFilePath string        // 源码输出文件
}

func NewWriteSource(tables []*Table, packageName string, outputFilePath string) *WriteSourceCode {
	result := &WriteSourceCode{
		Tables:         tables,
		PackageName:    packageName,
		SourceCode:     &bytes.Buffer{},
		OutputFilePath: outputFilePath,
	}
	return result
}

func (s *WriteSourceCode) WriteAll() (err error) {
	s.WritePublic()
	s.WriteTableStruct()
	s.WriteTableStructTab()
	s.WriteTableStructTabNew()
	s.WriteTableStructMethod()
	err = s.WriteFile()
	if err != nil {
		return
	}
	err = FmtGoFile(s.OutputFilePath)
	if err != nil {
		return
	}
	return
}

func (s *WriteSourceCode) WritePublic() {
	content := `package %s

import (
	"database/sql"
	"github.com/xooooooox/hey"
)
`
	content = fmt.Sprintf(content, s.PackageName)
	s.SourceCode.WriteString(content)
}

func (s *WriteSourceCode) WriteTableStruct() {
	tmp := bytes.Buffer{}
	tmp.WriteString("\n")
	for _, table := range s.Tables {
		tmp.WriteString(fmt.Sprintf("// %s %s %s\n", UnderlineToPascal(*(table.TableName)), *(table.TableName), table.Comment(true)))
		tmp.WriteString(fmt.Sprintf("type %s struct {\n", UnderlineToPascal(*(table.TableName))))
		for _, c := range table.Column {
			tmp.WriteString(fmt.Sprintf("\t%s %s", UnderlineToPascal(*c.ColumnName), c.DatabaseTypeToGoType()))
			tmp.WriteString("`")
			tmp.WriteString(fmt.Sprintf("json:\"%s\"", PascalToUnderline(*c.ColumnName)))
			tmp.WriteString(fmt.Sprintf(" db:\"%s\"", *c.ColumnName))
			tmp.WriteString("`")
			tmp.WriteString(c.Comment())
			tmp.WriteString("\n")
		}
		tmp.WriteString("}\n")
		tmp.WriteString("\n")
	}
	s.SourceCode.Write(tmp.Bytes())
}

func (s *WriteSourceCode) WriteTableStructTab() {
	template := `
type %s struct {
	Way     *hey.Way // *hey.Way
	Table   string   // %s
	%s
	Column  []string  // all columns of this table
}
`
	tmp := bytes.Buffer{}
	for _, table := range s.Tables {
		pascal := UnderlineToPascal(*(table.TableName))
		cols := make([]string, 0)
		for _, column := range table.Column {
			col := fmt.Sprintf("%s string //", UnderlineToPascal(*(column.ColumnName)))
			if column.ColumnComment != nil {
				col = fmt.Sprintf("%s %s", col, *column.ColumnComment)
			}
			cols = append(cols, col)
		}
		tableComment := ""
		if table.TableComment != nil {
			tableComment = *table.TableComment
		}
		tmp.WriteString(fmt.Sprintf(template,
			TabName(pascal),            // _Hello
			tableComment,               // table comment
			strings.Join(cols, "\n\t"), // columns
		))
		tmp.WriteString("\n")
	}
	s.SourceCode.Write(tmp.Bytes())
}

func (s *WriteSourceCode) WriteTableStructTabNew() {
	template := `
func New%s(w ...*hey.Way) (result *%s) {
	result = &%s{
		Way:   heyWay(w...),   // *hey.Way
		Table: "%s", 		   // %s
		%s
		Column: []string{%s},
	}
	return
}
`
	tmp := bytes.Buffer{}
	for _, table := range s.Tables {
		pascal := UnderlineToPascal(*(table.TableName))
		name := *(table.TableName)
		field := make([]string, 0)
		cols := make([]string, 0)
		for _, column := range table.Column {
			field = append(field, fmt.Sprintf("\"%s\"", *column.ColumnName))
			col := fmt.Sprintf("%s:\"%s\", //", UnderlineToPascal(*(column.ColumnName)), *(column.ColumnName))
			if column.ColumnComment != nil {
				col = fmt.Sprintf("%s %s", col, *column.ColumnComment)
			}
			cols = append(cols, col)
		}
		if table.TableSchema != nil && *table.TableSchema != "" {
			name = fmt.Sprintf("%s.%s", *table.TableSchema, *table.TableName)
		}
		nameComment := ""
		if table.TableComment != nil {
			nameComment = *(table.TableComment)
		}
		tmp.WriteString(fmt.Sprintf(template,
			pascal,                       // Hello
			TabName(pascal),              // TabHello
			TabName(pascal),              // TabHello
			name,                         // table name
			nameComment,                  // table comment
			strings.Join(cols, "\n\t\t"), // columns
			strings.Join(field, ", "),
		))
		tmp.WriteString("\n")
	}
	s.SourceCode.Write(tmp.Bytes())
}

func (s *WriteSourceCode) WriteTableStructMethod() {
	template := `
func (s *TabTableName) Insert(field []string, value ...[]interface{}) (int64, error) {
	prepare, args := hey.SqlInsert(s.Table, field, value...)
	return s.Way.Exec(prepare, args...)
}

func (s *TabTableName) Delete(where hey.Filter) (int64, error) {
	prepare, args := hey.SqlDelete(s.Table, where)
	return s.Way.Exec(prepare, args...)
}

func (s *TabTableName) Update(where hey.Filter, field []string, value []interface{}) (int64, error) {
	prepare, args := hey.SqlUpdate(s.Table, field, value, where)
	return s.Way.Exec(prepare, args...)
}

func (s *TabTableName) Select(fc func(q *hey.Selector) (err error)) error {
	return fc(hey.NewSelector().Table(s.Table))
}

func (s *TabTableName) SelectOne(fc ...func(q *hey.Selector)) (result *TableName, err error) {
	var n int64
	result = &TableName{}
	query := hey.NewSelector().Table(s.Table).Field(s.Column...)
	for _, fx := range fc {
		if fx != nil {
			fx(query)
		}
	}
	prepare, args := query.Limit(1).SqlSelect()
	if err = s.Way.Query(func(rows *sql.Rows) (err error) {
		n, err = RowsAssoc(rows, result)
		return
	}, prepare, args...); err != nil || n == 0 {
		result = nil
	}
	return
}

func (s *TabTableName) SelectAll(fc ...func(q *hey.Selector)) (result []*TableName, err error) {
	result = make([]*TableName, 0)
	query := hey.NewSelector().Table(s.Table).Field(s.Column...)
	for _, fx := range fc {
		if fx != nil {
			fx(query)
		}
	}
	prepare, args := query.SqlSelect()
	err = s.Way.Query(func(rows *sql.Rows) (err error) {
		_, err = RowsAssoc(rows, &result)
		return
	}, prepare, args...)
	return
}
`
	tmp := bytes.Buffer{}
	for _, table := range s.Tables {
		pascal := UnderlineToPascal(*(table.TableName))
		content := template
		content = strings.ReplaceAll(content, "TableName", pascal)
		tmp.WriteString(content)
		tmp.WriteString("\n")
	}
	s.SourceCode.Write(tmp.Bytes())
}

func (s *WriteSourceCode) WriteFile() error {
	dir := path.Dir(s.OutputFilePath)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0744)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	var fil *os.File
	fil, err = os.Create(s.OutputFilePath)
	if err != nil {
		return err
	}
	defer fil.Close()
	_, err = io.Copy(fil, s.SourceCode)
	return err
}

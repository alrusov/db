/*
Работа с базами данных
*/
package db

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/alrusov/initializer"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
	"github.com/alrusov/panic"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	// Список используемых баз данных. Ключ - имя соединения
	Config map[string]*DB

	// Описание базы
	DB struct {
		Name           string         `toml:"-"`
		Active         bool           `toml:"active"`
		Driver         string         `toml:"driver"`
		DSN            string         `toml:"dsn"`
		UseQueriesFrom string         `toml:"use-queries-from"` // Use queries from another
		Queries        misc.StringMap `toml:"queries"`          // SQL запросы для этой базы, ключ - имя запроса

		conn *sqlx.DB
	}

	PatternType int

	SubstArg struct {
		name  string
		value any
	}

	Bulk [][]any
)

const (
	SubstJbFields  = "JB"
	SubstExtra     = "EXTRA"
	SubstExtraFrom = "EXTRA_FROM"

	PatternNames         = "@NAMES@"
	PatternNamesPreComma = "@NAMES_PRE_COMMA@"
	PatternVals          = "@VALS@"
	PatternValsPreComma  = "@VALS_PRE_COMMA@"
	PatternPairs         = "@PAIRS@"
	PatternPairsPreComma = "@PAIRS_PRE_COMMA@"
	PatternExtra         = "@" + SubstExtra + "@"
	PatternExtraFrom     = "@" + SubstExtraFrom + "@"
)

const (
	PatternTypeNone PatternType = iota
	PatternTypeSelect
	PatternTypeInsert
	PatternTypeUpdate
)

var (
	Log = log.NewFacility("db") // Log facility

	lastID uint64

	knownCfg *Config
)

//----------------------------------------------------------------------------------------------------------------------------//

func init() {
	// Регистрируем инициализатор
	initializer.RegisterModuleInitializer(initModule)
}

// Инициализация
func initModule(appCfg any, h any) (err error) {
	if knownCfg != nil {
		err = knownCfg.ConnectAll()
		if err != nil {
			return
		}
	}

	Log.Message(log.INFO, "Initialized")
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func Subst(name string, value any) *SubstArg {
	return &SubstArg{
		name:  name,
		value: value,
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности Config
func (x *Config) Check(cfg any) (err error) {
	knownCfg = x

	msgs := misc.NewMessages()

	for name, df := range *x {
		if !df.Active {
			delete(*x, name)
			continue
		}

		df.Name = name
		err = df.Check(cfg)
		if err != nil {
			msgs.Add("db.%s: %s", name, err)
		}

		if df.UseQueriesFrom != "" {
			c, exists := (*x)[df.UseQueriesFrom]
			if !exists {
				msgs.Add(`db.%s: queries source "%s" not found`, name, df.UseQueriesFrom)
			} else {
				df.Queries = c.Queries
			}
		}
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности DB
func (x *DB) Check(cfg any) (err error) {
	msgs := misc.NewMessages()

	if x.Driver == "" {
		msgs.Add("empty driver")
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) Connect() (conn *sqlx.DB, doRetry bool, err error) {
	doRetry = true

	conn, err = sqlx.Open(db.Driver, db.DSN)
	if err != nil {
		return
	}

	err = conn.Ping()
	if err != nil {
		return
	}

	err = newStat(db.Name)
	if err != nil {
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (c Config) ConnectAll() (err error) {
	msgs := misc.NewMessages()

	wg := new(sync.WaitGroup)
	wg.Add(len(c))

	for _, db := range c {
		db := db

		go func() {
			panicID := panic.ID()
			defer panic.SaveStackToLogEx(panicID)

			defer wg.Done()

			if db.DSN == "" {
				Log.Message(log.INFO, "[%s] empty DSN, skipped", db.Name)
				return
			}

			for misc.AppStarted() {
				conn, doRetry, err := db.Connect()
				if err != nil {
					Log.Message(log.WARNING, "[%s] %s", db.Name, err)
					if !doRetry {
						return
					}
					misc.Sleep(5 * time.Second)
					continue
				}

				db.conn = conn

				Log.Message(log.INFO, "[%s] database connection created", db.Name)

				misc.AddExitFunc(
					fmt.Sprintf("db[%s]", db.Name),
					func(_ int, _ any) {
						conn.Close()
						Log.Message(log.INFO, "[%s] database connection closed", db.Name)
					},
					nil,
				)

				break
			}
		}()
	}

	wg.Wait()

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

func GetDB(dbName string) (db *DB, err error) {
	db, exists := (*knownCfg)[dbName]
	if !exists {
		err = fmt.Errorf("unknown database %s", dbName)
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func GetConn(dbName string) (conn *sqlx.DB, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	conn = db.conn

	if conn == nil {
		err = fmt.Errorf("database %s is not connected", dbName)
		return

	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) GetQuery(queryName string) (q string, err error) {
	q, exists := db.Queries[queryName]
	if !exists {
		err = fmt.Errorf("query %s.%s not found", db.Name, queryName)
		return
	}

	return
}

func GetQuery(dbName string, queryName string) (q string, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.GetQuery(queryName)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) Query(dest any, queryName string, fields []string, vars []any) (err error) {
	t0 := misc.NowUnixNano()

	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %s", queryName, err)
		}
	}()

	db.statBegin(false)
	defer func() {
		db.statEnd(false, time.Duration(misc.NowUnixNano()-t0))
	}()

	id := atomic.AddUint64(&lastID, 1)
	logSrc := fmt.Sprintf("Q.%s.%d", db.Name, id)

	if Log.CurrentLogLevel() >= log.TIME {
		defer misc.LogProcessingTime(Log.Name(), "", id, db.Name, "", t0)
		Log.MessageWithSource(log.TRACE2, logSrc, "%s %v", queryName, vars)
	}

	if dest == nil {
		err = fmt.Errorf("destination is nil")
		return
	}

	if k := reflect.ValueOf(dest).Kind(); k != reflect.Pointer {
		err = fmt.Errorf(`dest is not a pointer (%T)`, dest)
		return
	}

	conn, preparedVars, _, q, err := db.prepareQuery(queryName, vars)
	if err != nil {
		return
	}

	if len(fields) == 0 {
		fields = []string{"*"}
	}

	q = fillPatterns(q, PatternTypeSelect, 0, fields)

	return db.query(conn, nil, dest, q, preparedVars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) query(conn *sqlx.DB, tx *sqlx.Tx, dest any, q string, preparedVars []any) (err error) {
	switch dest := dest.(type) {
	default:
		if k := reflect.Indirect(reflect.ValueOf(dest)).Kind(); k == reflect.Slice {
			// Слайс [предположительно] структур - пытаемся залить данные туда
			if tx == nil {
				err = conn.Select(dest, q, preparedVars...)
			} else {
				err = tx.Select(dest, q, preparedVars...)
			}
			return
		}

		err = fmt.Errorf(`illegal type of the dest (%T)`, dest)
		return

	case *([]misc.InterfaceMap):
		// Иначе - зальем в []InterfaceMap

		var rows *sqlx.Rows
		if tx == nil {
			rows, err = conn.Queryx(q, preparedVars...)
		} else {
			rows, err = tx.Queryx(q, preparedVars...)
		}
		if err != nil {
			return err
		}

		defer rows.Close()

		var cols []string
		cols, err = rows.Columns()
		if err != nil {
			return err
		}

		nCols := len(cols)

		result := make([]misc.InterfaceMap, 0, 8)

		vals := make([]any, nCols)
		for i := range vals {
			var v any
			vals[i] = &v
		}

		for rows.Next() {
			err = rows.Scan(vals...)
			if err != nil {
				return
			}

			r := make(misc.InterfaceMap, nCols)
			for i, name := range cols {
				r[name] = *(vals[i].(*any))
			}

			result = append(result, r)
		}

		*dest = result
		return
	}
}

func Query(dbName string, dest any, queryName string, fields []string, vars []any) (err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.Query(dest, queryName, fields, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

type Result struct {
	ids  []int64
	rows int64
}

func (r Result) AllInsertId() ([]int64, error) {
	return r.ids, nil
}

func (r Result) LastInsertId() (int64, error) {
	ln := len(r.ids)
	if ln == 0 {
		return 0, nil
	}

	return r.ids[ln-1], nil
}

func (r Result) RowsAffected() (int64, error) {
	return r.rows, nil
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) ExecEx(dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result sql.Result, err error) {
	t0 := misc.NowUnixNano()

	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %s", queryName, err)
		}
	}()

	db.statBegin(true)
	defer func() {
		db.statEnd(true, time.Duration(misc.NowUnixNano()-t0))
	}()

	id := atomic.AddUint64(&lastID, 1)
	logSrc := fmt.Sprintf("E.%s.%d", db.Name, id)

	if Log.CurrentLogLevel() >= log.TIME {
		defer misc.LogProcessingTime(Log.Name(), "", id, db.Name, "", t0)
		Log.MessageWithSource(log.TRACE2, logSrc, "%s %v", queryName, vars)
	}

	conn, preparedVars, bulk, q, err := db.prepareQuery(queryName, vars)
	if err != nil {
		return
	}

	q = fillPatterns(q, tp, firstDataFieldIdx, fields)

	ctx := context.Background()

	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}

		err = tx.Commit()
	}()

	withDest := !(dest == nil || reflect.ValueOf(dest).IsNil())

	if bulk == nil {
		// simple exec

		if withDest {
			err = db.query(conn, tx, dest, q, preparedVars)
			result = nil
		} else {
			result, err = tx.ExecContext(ctx, q, preparedVars...)
		}

		return
	}

	// bulk exec (usually an insert)

	stmt, err := tx.Preparex(q)
	if err != nil {
		return
	}

	defer stmt.Close()

	if withDest {
		t := reflect.TypeOf(dest)
		if t.Kind() != reflect.Ptr {
			err = fmt.Errorf("dest %T is not a pointer", dest)
			return
		}

		t = t.Elem()

		if t.Kind() != reflect.Slice {
			err = fmt.Errorf("dest %T is not a pointer to slice", dest)
			return
		}

		qDestV := reflect.New(t)
		qDest := qDestV.Interface()

		allDestV := reflect.New(t).Elem()

		for _, vars := range bulk {
			err = stmt.SelectContext(ctx, qDest, vars...)
			if err != nil {
				return
			}

			allDestV = reflect.AppendSlice(allDestV, qDestV.Elem())
		}

		reflect.ValueOf(dest).Elem().Set(allDestV)
		return
	}

	// without dest

	res := &Result{
		ids: make([]int64, 0, len(bulk)),
	}

	for _, vars := range bulk {
		var r sql.Result
		r, err = stmt.ExecContext(ctx, vars...)
		if err != nil {
			return
		}

		id, e := r.LastInsertId()
		if e == nil {
			res.ids = append(res.ids, id)
		}

		n, e := r.RowsAffected()
		if e == nil {
			res.rows += n
		}
	}

	result = res

	return
}

func ExecEx(dbName string, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result sql.Result, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.ExecEx(dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func (db *DB) Exec(queryName string, vars []any) (result sql.Result, err error) {
	return db.ExecEx(nil, queryName, PatternTypeNone, 0, nil, vars)
}

func Exec(dbName string, queryName string, vars []any) (result sql.Result, err error) {
	return ExecEx(dbName, nil, queryName, PatternTypeNone, 0, nil, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) prepareQuery(queryName string, vars []any) (conn *sqlx.DB, preparedVars []any, bulk Bulk, q string, err error) {
	if queryName != "" && queryName[0] == '#' {
		q = queryName[1:]
	} else {
		q, err = db.GetQuery(queryName)
		if err != nil {
			return
		}
	}

	conn, err = GetConn(db.Name)
	if err != nil {
		return
	}

	q, vars, err = doSubst(q, vars)
	if err != nil {
		return
	}

	preparedVars = make([]any, len(vars))
	copy(preparedVars, vars)

	for _, v := range preparedVars {
		switch v := v.(type) {
		case Bulk:
			bulk = append(bulk, v...)

		default:
			if reflect.ValueOf(v).Kind() == reflect.Slice {
				q, preparedVars, err = sqlx.In(q, preparedVars...)
				if err != nil {
					return
				}

				q = conn.Rebind(q)
				break
			}
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func doSubst(q string, vars []any) (newQ string, newVars []any, err error) {
	newQ = q

	if len(vars) == 0 {
		newVars = vars
		return
	}

	newVars = make([]any, 0, len(vars))

	for _, v := range vars {
		switch v := v.(type) {
		default:
			newVars = append(newVars, v)

		case *SubstArg:
			if v == nil {
				err = fmt.Errorf(`subst is nil`)
				return
			}

			var s string

			switch val := v.value.(type) {
			default:
				s, err = misc.Iface2String(val)
				if err != nil {
					err = fmt.Errorf(`subst %#v: %s`, v, err)
					return
				}

			case JbPairs:
				ln := len(val)
				vv := make([]string, ln)
				for i := 0; i < ln; i++ {
					f := val[i]
					vv[i] = fmt.Sprintf(f.Format, f.Idx)
				}

				s = strings.Join(vv, ",")
			}

			newQ = strings.ReplaceAll(newQ, "@"+v.name+"@", s)
		}
	}

	newQ = strings.ReplaceAll(newQ, PatternExtra, "")     // if not substituted before
	newQ = strings.ReplaceAll(newQ, PatternExtraFrom, "") // if not substituted before

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func fillPatterns(q string, tp PatternType, firstDataFieldIdx int, fields []string) string {
	nf := len(fields)

	switch tp {
	case PatternTypeNone:

	case PatternTypeSelect:
		f := strings.Join(fields, ",")
		q = strings.ReplaceAll(q, PatternNames, f)

	case PatternTypeInsert:
		f := strings.Join(fields, ",")

		vv := make([]string, nf)
		for i := 0; i < nf; i++ {
			vv[i] = "$" + strconv.FormatInt(int64(firstDataFieldIdx+i), 10)
		}
		v := strings.Join(vv, ",")

		q = strings.ReplaceAll(q, PatternNames, f)
		q = strings.ReplaceAll(q, PatternVals, v)

		if len(f) > 0 {
			f = "," + f
			v = "," + v
		}
		q = strings.ReplaceAll(q, PatternNamesPreComma, f)
		q = strings.ReplaceAll(q, PatternValsPreComma, v)

	case PatternTypeUpdate:
		vv := make([]string, nf)
		for i := 0; i < nf; i++ {
			vv[i] = fmt.Sprintf("%s=$%d", fields[i], firstDataFieldIdx+i)
		}

		v := strings.Join(vv, ",")
		q = strings.ReplaceAll(q, PatternPairs, v)

		if len(v) > 0 {
			v = "," + v
		}
		q = strings.ReplaceAll(q, PatternPairsPreComma, v)
	}

	return q
}

//----------------------------------------------------------------------------------------------------------------------------//

/*
Работа с базами данных
*/
package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"

	"github.com/alrusov/initializer"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
	"github.com/alrusov/panic"
)

//----------------------------------------------------------------------------------------------------------------------------//

// jb works with pgsql only!

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
		mock sqlmock.Sqlmock
	}

	PatternType int

	SubstArg struct {
		name  string
		value any
	}

	Bulk [][]any

	MockCallback func(db *DB, mock sqlmock.Sqlmock, q string, v []any) (err error)
)

const (
	SubstNames         = "NAMES"
	SubstNamesPreComma = "NAMES_PRE_COMMA"
	SubstVals          = "VALS"
	SubstValsPreComma  = "VALS_PRE_COMMA"
	SubstPairs         = "PAIRS"
	SubstPairsPreComma = "PAIRS_PRE_COMMA"
	SubstJbFields      = "JB"
	SubstExtra         = "EXTRA"
	SubstExtraFrom     = "EXTRA_FROM"
	SubstExtraFullFrom = "EXTRA_FULL_FROM"

	PatternNames         = "@" + SubstNames + "@"
	PatternNamesPreComma = "@" + SubstNamesPreComma + "@"
	PatternVals          = "@" + SubstVals + "@"
	PatternValsPreComma  = "@" + SubstValsPreComma + "@"
	PatternPairs         = "@" + SubstPairs + "@"
	PatternPairsPreComma = "@" + SubstPairsPreComma + "@"
	PatternExtra         = "@" + SubstExtra + "@"
	PatternExtraFrom     = "@" + SubstExtraFrom + "@"
	PatternExtraFullFrom = "@" + SubstExtraFullFrom + "@"
)

const (
	PatternTypeNone PatternType = iota
	PatternTypeSelect
	PatternTypeInsert
	PatternTypeUpdate
)

var (
	Log = log.NewFacility("db") // Log facility

	disabled = false

	lastID uint64

	knownCfg *Config

	mockEnabled  = false
	mockCallback = MockCallback(nil)
)

//----------------------------------------------------------------------------------------------------------------------------//

func init() {
	// Регистрируем инициализатор
	initializer.RegisterModuleInitializer(initModule)
}

// Инициализация
func initModule(appCfg any, h any) (err error) {
	if disabled {
		return
	}

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

func Disable() {
	disabled = true
}

func Disabled() bool {
	return disabled
}

//----------------------------------------------------------------------------------------------------------------------------//

func Subst(name string, value any) *SubstArg {
	return &SubstArg{
		name:  name,
		value: value,
	}
}

func (a *SubstArg) Name() string {
	return a.name
}

func (a *SubstArg) Value() any {
	return a.value
}

func (a *SubstArg) SetValue(v any) {
	a.value = v
}

func (a *SubstArg) String() string {
	return fmt.Sprintf("{SubstArg(%s)=`%v`}", a.name, a.value)
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

func EnableMock() {
	mockEnabled = true
}

func IsMockEnabled() bool {
	return mockEnabled
}

func SetMockCallback(f MockCallback) {
	mockCallback = f
}

//----------------------------------------------------------------------------------------------------------------------------//

// Сrooked workaround for the mock NamedArg with slice problem

type mockBlackHole struct{}

func (bh mockBlackHole) ConvertValue(v any) (driver.Value, error) {
	return nil, nil
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) Connect() (doRetry bool, err error) {
	doRetry = true

	var conn *sqlx.DB
	var mock sqlmock.Sqlmock

	if mockEnabled {
		var stdDB *sql.DB

		stdDB, mock, err = sqlmock.New(sqlmock.ValueConverterOption(mockBlackHole{}))
		if err != nil {
			return
		}

		conn = sqlx.NewDb(stdDB, db.Driver)

	} else {
		conn, err = sqlx.Open(db.Driver, db.DSN)
		if err != nil {
			return
		}

		err = conn.Ping()
		if err != nil {
			return
		}
	}

	err = newStat(db.Name)
	if err != nil {
		return
	}

	db.conn = conn
	db.mock = mock

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
				doRetry, err := db.Connect()
				if err != nil {
					Log.Message(log.WARNING, "[%s] %s", db.Name, err)
					if !doRetry {
						return
					}
					misc.Sleep(5 * time.Second)
					continue
				}

				Log.Message(log.INFO, "[%s] database connection created", db.Name)

				misc.AddExitFunc(
					fmt.Sprintf("db[%s]", db.Name),
					func(_ int, _ any) {
						db.conn.Close()
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
	return db.QueryWithMock(nil, dest, queryName, fields, vars)
}

func (db *DB) QueryWithMock(mock MockCallback, dest any, queryName string, fields []string, vars []any) (err error) {
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
		defer func() {
			if err != nil {
				Log.MessageWithSource(log.TRACE1, logSrc, "%s %s", queryName, err)
			} else {
				v := reflect.ValueOf(dest)
				if v.Kind() == reflect.Pointer {
					v = v.Elem()
				}
				switch v.Kind() {
				case reflect.Slice,
					reflect.Map:
					Log.MessageWithSource(log.TRACE1, logSrc, "%s got %d rows", queryName, v.Len())
				}
			}
			misc.LogProcessingTime(Log.Name(), "", id, db.Name, "", t0)
		}()

		Log.MessageWithSource(log.TRACE1, logSrc, "%s %v", queryName, vars)
	}

	if dest == nil {
		err = fmt.Errorf("destination is nil")
		return
	}

	if k := reflect.ValueOf(dest).Kind(); k != reflect.Pointer {
		err = fmt.Errorf(`dest is not a pointer (%T)`, dest)
		return
	}

	conn, preparedVars, _, q, err := db.prepareQuery(queryName, PatternTypeSelect, vars)
	if err != nil {
		return
	}

	if len(fields) == 0 {
		fields = []string{"*"}
	}

	q = fillPatterns(q, PatternTypeSelect, 0, fields)

	if Log.CurrentLogLevel() >= log.TRACE4 {
		Log.Message(log.TRACE4, "query: %s", q)
	}

	return db.query(mock, conn, nil, dest, q, preparedVars)
}

func Query(dbName string, dest any, queryName string, fields []string, vars []any) (err error) {
	return QueryWithMock(nil, dbName, dest, queryName, fields, vars)
}

func QueryWithMock(mock MockCallback, dbName string, dest any, queryName string, fields []string, vars []any) (err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.QueryWithMock(mock, dest, queryName, fields, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) query(mock MockCallback, conn *sqlx.DB, tx *sqlx.Tx, dest any, q string, preparedVars []any) (err error) {
	if db.mock == nil {
		mock = nil
	} else if mock == nil {
		mock = mockCallback
	}

	switch dest := dest.(type) {
	default:
		if k := reflect.Indirect(reflect.ValueOf(dest)).Kind(); k == reflect.Slice {
			// Слайс [предположительно] структур - пытаемся залить данные туда

			if mock != nil {
				err = mock(db, db.mock, q, preparedVars)
				if err != nil {
					return
				}
			}

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

		if mock != nil {
			err = mock(db, db.mock, q, preparedVars)
			if err != nil {
				return
			}
		}

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

func (db *DB) Exec(queryName string, vars []any) (result sql.Result, err error) {
	return db.ExecExWithMock(nil, nil, queryName, PatternTypeNone, 0, nil, vars)
}

func (db *DB) ExecEx(dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result sql.Result, err error) {
	return db.ExecExWithMock(nil, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func (db *DB) ExecExWithMock(mock MockCallback, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result sql.Result, err error) {
	t0 := misc.NowUnixNano()

	if db.mock == nil {
		mock = nil
	} else if mock == nil {
		mock = mockCallback
	}

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
		defer func() {
			if err != nil {
				Log.MessageWithSource(log.TRACE1, logSrc, "%s %s", queryName, err)
			}
			misc.LogProcessingTime(Log.Name(), "", id, db.Name, "", t0)
		}()

		Log.MessageWithSource(log.TRACE1, logSrc, "%s %v", queryName, vars)
	}

	conn, preparedVars, bulk, q, err := db.prepareQuery(queryName, tp, vars)
	if err != nil {
		return
	}

	q = fillPatterns(q, tp, firstDataFieldIdx, fields)

	if Log.CurrentLogLevel() >= log.TRACE4 {
		Log.Message(log.TRACE4, "exec: %s", q)
	}

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
			err = db.query(mock, conn, tx, dest, q, preparedVars)
			result = nil
		} else {

			if mock != nil {
				err = mock(db, db.mock, q, preparedVars)
				if err != nil {
					return
				}
			}

			result, err = tx.ExecContext(ctx, q, preparedVars...)
		}

		return
	}

	// bulk exec (usually an insert)

	if mock != nil {
		err = mock(db, db.mock, q, preparedVars)
		if err != nil {
			return
		}
	}

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

func Exec(dbName string, queryName string, vars []any) (result sql.Result, err error) {
	return ExecExWithMock(nil, dbName, nil, queryName, PatternTypeNone, 0, nil, vars)
}

func ExecEx(dbName string, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result sql.Result, err error) {
	return ExecExWithMock(nil, dbName, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func ExecExWithMock(mock MockCallback, dbName string, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result sql.Result, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.ExecExWithMock(mock, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) prepareQuery(queryName string, tp PatternType, vars []any) (conn *sqlx.DB, preparedVars []any, bulk Bulk, q string, err error) {
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

	q, vars, err = doSubst(q, tp, vars)
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

func doSubst(q string, tp PatternType, vars []any) (newQ string, newVars []any, err error) {
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

			case JbPairs: // for pgsql only...
				s = val.String(tp)
			}

			newQ = strings.ReplaceAll(newQ, "@"+v.name+"@", s)
		}
	}

	newQ = strings.ReplaceAll(newQ, PatternExtra, "")         // if not substituted before
	newQ = strings.ReplaceAll(newQ, PatternExtraFrom, "")     // if not substituted before
	newQ = strings.ReplaceAll(newQ, PatternExtraFullFrom, "") // if not substituted before

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

var maxFuncPairsCount = 50

func (jbp JbPairs) String(tp PatternType) (s string) {
	_, s = jbp.block2string(tp, 0, "")
	return
}

func (jbp JbPairs) block2string(tp PatternType, startIdx int, container string) (idx int, res string) {
	var sb strings.Builder

	defer func() {
		sb.WriteString(")::jsonb)")
		res = sb.String()
	}()

	n := 0
	ln := len(jbp)

	for idx = startIdx; idx < ln; {
		f := jbp[idx]

		prefix := ","
		if n%maxFuncPairsCount == 0 {
			if n == 0 {
				prefix = "(jsonb_build_object("
			} else {
				prefix = ")||jsonb_build_object("
			}
		}

		if container == f.FieldInfo.Container {
			sb.WriteString(prefix)
			sb.WriteString(fmt.Sprintf(f.Format, f.Idx))

			idx++
			n++
			continue
		}

		if container != "" && !strings.HasPrefix(f.FieldInfo.Container, container+".") {
			return idx, ""
		}

		var s string
		idx, s = jbp.block2string(tp, idx, f.FieldInfo.Container)

		sb.WriteString(prefix)
		sb.WriteByte('\'')
		sb.WriteString(f.FieldInfo.Container)
		sb.WriteByte('\'')
		sb.WriteByte(',')

		if tp == PatternTypeUpdate && f.FieldInfo.Parent != nil && f.FieldInfo.Parent.Container != "" {
			sb.WriteString(fmt.Sprintf("COALESCE(%s->'%s', '{}'::jsonb)||", f.FieldInfo.Parent.Container, f.FieldInfo.Container))
		}

		sb.WriteString(s)

		n++
	}

	return idx, ""
}

//----------------------------------------------------------------------------------------------------------------------------//

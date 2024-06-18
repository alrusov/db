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
	"github.com/jmoiron/sqlx/reflectx"

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

	Result struct {
		ids      []int64
		rows     int64
		errors   []error
		hasError bool
	}

	PatternType int

	SubstArg struct {
		name  string
		value any
	}

	Bulk [][]any

	MockCallback func(db *DB, mock sqlmock.Sqlmock, q string, v []any) (err error)

	Error struct {
		message string
		parent  error
	}

	executor interface {
		Queryx(query string, args ...any) (*sqlx.Rows, error)
		Select(dest any, query string, args ...any) error
		Exec(query string, arg ...any) (sql.Result, error)
		Preparex(query string) (*sqlx.Stmt, error)
	}
)

const (
	TagDB    = "db"
	TagDBAlt = "dbAlt"

	SubstTable         = "TABLE"
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
	SubstBefore        = "BEFORE"
	SubstAfter         = "AFTER"

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

	altTagsEnabled = false

	disabled = false

	lastID uint64

	knownCfg *Config

	mockEnabled  = false
	mockCallback = MockCallback(nil)

	cleanupPatterns = misc.StringMap{
		PatternExtra:         "",
		PatternExtraFrom:     "",
		PatternExtraFullFrom: "",
	}
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

func NewError(parentError error, msg string, params ...any) (e *Error) {
	if parentError != nil && msg == "" {
		msg = parentError.Error()
	} else {
		msg = fmt.Sprintf(msg, params...)
	}

	return &Error{
		message: msg,
		parent:  parentError,
	}
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) String() string {
	return e.message
}

func (e *Error) Parent() error {
	return e.parent
}

//----------------------------------------------------------------------------------------------------------------------------//

func (r *Result) Add(sqlResult sql.Result, err error) {
	r.errors = append(r.errors, err) // with nils
	if err != nil {
		r.hasError = true
	}

	if sqlResult == nil {
		r.ids = append(r.ids, 0)
		if err == nil {
			r.rows++
		}
		return
	}

	id, _ := sqlResult.LastInsertId()
	r.ids = append(r.ids, id) // with 0

	n, err := sqlResult.RowsAffected()
	if err == nil {
		r.rows += n
	}
}

func (r *Result) LastInsertId() (int64, error) {
	if r == nil {
		return 0, nil
	}

	ln := len(r.ids)
	if ln == 0 {
		return 0, nil
	}

	return r.ids[ln-1], nil
}

func (r *Result) RowsAffected() (int64, error) {
	if r == nil {
		return 0, nil
	}

	return r.rows, nil
}

func (r *Result) Errors() []error {
	if r == nil {
		return nil
	}

	return r.errors
}

func (r *Result) HasError() bool {
	if r == nil {
		return false
	}

	return r.hasError
}

//----------------------------------------------------------------------------------------------------------------------------//

func Disable() {
	disabled = true
}

func Disabled() bool {
	return disabled
}

//----------------------------------------------------------------------------------------------------------------------------//

func AddCleanupPatterns(p misc.StringMap) {
	for name, val := range p {
		cleanupPatterns["@"+name+"@"] = val
	}
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

func EnableAltTags() {
	altTagsEnabled = true
}

func IsAltTagsEnabled() bool {
	return altTagsEnabled
}

func Tag() string {
	if altTagsEnabled {
		return TagDBAlt
	}

	return TagDB
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

	defer func() {
		if err != nil {
			err = NewError(err, "")
		}
	}()

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

	if altTagsEnabled {
		conn.Mapper = reflectx.NewMapperFunc(TagDBAlt, strings.ToLower)
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
		err = NewError(nil, "unknown database %s", dbName)
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) GetConn() (conn *sqlx.DB, err error) {
	conn = db.conn

	if conn == nil {
		err = NewError(nil, "database %s is not connected", db.Name)
		return
	}

	return
}

func GetConn(dbName string) (conn *sqlx.DB, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.GetConn()
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) GetQuery(queryName string) (q string, err error) {
	q, exists := db.Queries[queryName]
	if !exists {
		err = NewError(nil, "query %s.%s not found", db.Name, queryName)
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
	return db.QueryTxWithMock(nil, nil, dest, queryName, fields, vars)
}

func (db *DB) QueryTx(tx *sqlx.Tx, dest any, queryName string, fields []string, vars []any) (err error) {
	return db.QueryTxWithMock(nil, tx, dest, queryName, fields, vars)
}

func (db *DB) QueryWithMock(mock MockCallback, dest any, queryName string, fields []string, vars []any) (err error) {
	return db.QueryTxWithMock(mock, nil, dest, queryName, fields, vars)
}

func (db *DB) QueryTxWithMock(mock MockCallback, tx *sqlx.Tx, dest any, queryName string, fields []string, vars []any) (err error) {
	t0 := misc.NowUnixNano()

	db.statBegin(false)

	defer func() {
		if err != nil {
			err = NewError(err, "%s: %s", queryName, err)
		}

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

	preparedVars, _, q, err := db.prepareQuery(queryName, PatternTypeSelect, vars)
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

	return db.query(mock, tx, dest, q, preparedVars)
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

func (db *DB) query(mock MockCallback, tx *sqlx.Tx, dest any, q string, preparedVars []any) (err error) {
	var ex executor
	ex = tx
	if tx == nil {
		ex = db.conn
	}

	if db.mock == nil {
		mock = nil
	} else if mock == nil {
		mock = mockCallback
	}

	if mock != nil {
		err = mock(db, db.mock, q, preparedVars)
		if err != nil {
			return
		}
	}

	switch dest := dest.(type) {
	default:
		if k := reflect.Indirect(reflect.ValueOf(dest)).Kind(); k == reflect.Slice {
			// Слайс [предположительно] структур - пытаемся залить данные туда

			err = ex.Select(dest, q, preparedVars...)
			return
		}

		err = fmt.Errorf(`illegal type of the dest (%T)`, dest)
		return

	case **sqlx.Rows:
		var rows *sqlx.Rows
		rows, err = ex.Queryx(q, preparedVars...)
		if err != nil {
			return err
		}

		*dest = rows
		return

	case *([]misc.InterfaceMap):
		// Иначе - зальем в []InterfaceMap

		var rows *sqlx.Rows
		rows, err = ex.Queryx(q, preparedVars...)
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

func (db *DB) Exec(queryName string, vars []any) (result *Result, err error) {
	return db.ExecTxExWithMock(nil, nil, nil, queryName, PatternTypeNone, 0, nil, vars)
}

func (db *DB) ExecTx(tx *sqlx.Tx, queryName string, vars []any) (result *Result, err error) {
	return db.ExecTxExWithMock(nil, tx, nil, queryName, PatternTypeNone, 0, nil, vars)
}

func (db *DB) ExecEx(dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result *Result, err error) {
	return db.ExecTxExWithMock(nil, nil, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func (db *DB) ExecTxEx(tx *sqlx.Tx, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result *Result, err error) {
	return db.ExecTxExWithMock(nil, tx, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func (db *DB) ExecExWithMock(mock MockCallback, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result *Result, err error) {
	return db.ExecTxExWithMock(mock, nil, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func (db *DB) ExecTxExWithMock(mock MockCallback, tx *sqlx.Tx, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result *Result, err error) {
	t0 := misc.NowUnixNano()

	var ex executor
	ex = tx
	if tx == nil {
		ex = db.conn
	}

	defer func() {
		if err != nil {
			err = NewError(err, "%s: %s", queryName, err)
		}
	}()

	if db.mock == nil {
		mock = nil
	} else if mock == nil {
		mock = mockCallback
	}

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

	preparedVars, bulk, q, err := db.prepareQuery(queryName, tp, vars)
	if err != nil {
		return
	}

	q = fillPatterns(q, tp, firstDataFieldIdx, fields)

	if Log.CurrentLogLevel() >= log.TRACE4 {
		Log.Message(log.TRACE4, "exec: %s", q)
	}

	ctx := context.Background()

	withDest := !(dest == nil || reflect.ValueOf(dest).IsNil())

	if bulk == nil {
		// simple exec

		result = &Result{}

		if withDest {
			e := db.query(mock, tx, dest, q, preparedVars)
			result.Add(nil, e)
		} else {
			if mock != nil {
				err = mock(db, db.mock, q, preparedVars)
				if err != nil {
					return
				}
			}

			var r sql.Result
			r, e := ex.Exec(q, preparedVars...)
			result.Add(r, e)
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

	stmt, err := ex.Preparex(q)
	if err != nil {
		return
	}

	defer stmt.Close()

	result = &Result{
		ids:    make([]int64, 0, len(bulk)),
		errors: make([]error, 0, len(bulk)),
	}

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

		allDestV := reflect.New(t).Elem()

		for _, vars := range bulk {
			qDestV := reflect.New(t)
			qDest := qDestV.Interface()

			e := stmt.SelectContext(ctx, qDest, vars...)
			result.Add(nil, e)

			var res reflect.Value
			qe := qDestV.Elem()
			if qe.Len() != 0 {
				res = qe.Index(0)
			} else {
				res = reflect.New(qe.Type().Elem()).Elem()
			}

			allDestV = reflect.Append(allDestV, res)
		}

		reflect.ValueOf(dest).Elem().Set(allDestV)
		return
	}

	// without dest

	for _, vars := range bulk {
		var r sql.Result
		r, e := stmt.ExecContext(ctx, vars...)
		result.Add(r, e)
	}

	return
}

func Exec(dbName string, queryName string, vars []any) (result *Result, err error) {
	return ExecExWithMock(nil, dbName, nil, queryName, PatternTypeNone, 0, nil, vars)
}

func ExecEx(dbName string, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result *Result, err error) {
	return ExecExWithMock(nil, dbName, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

func ExecExWithMock(mock MockCallback, dbName string, dest any, queryName string, tp PatternType, firstDataFieldIdx int, fields []string, vars []any) (result *Result, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.ExecExWithMock(mock, dest, queryName, tp, firstDataFieldIdx, fields, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) prepareQuery(queryName string, tp PatternType, vars []any) (preparedVars []any, bulk Bulk, q string, err error) {
	if queryName != "" && queryName[0] == '#' {
		q = queryName[1:]
	} else {
		q, err = db.GetQuery(queryName)
		if err != nil {
			return
		}
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

				q = db.conn.Rebind(q)
				break
			}
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func doSubst(q string, tp PatternType, vars []any) (newQ string, newVars []any, err error) {
	newQ = q
	before := ""
	after := ""

	defer func() {
		for name, val := range cleanupPatterns {
			newQ = strings.ReplaceAll(newQ, name, val) // if not substituted before
		}

		if before != "" || after != "" {
			qq := make([]string, 0, 3)

			if before != "" {
				qq = append(qq, before)
			}

			qq = append(qq, newQ)

			if after != "" {
				qq = append(qq, after)
			}

			newQ = strings.Join(qq, ";")
		}
	}()

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

				switch v.name {
				default:
					// nothing to do
				case SubstBefore:
					before = s
					continue
				case SubstAfter:
					after = s
					continue
				}

			case JbPairs: // for pgsql only...
				s = val.String(tp)
			}

			newQ = strings.ReplaceAll(newQ, "@"+v.name+"@", s)
		}
	}

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

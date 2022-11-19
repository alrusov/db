/*
Работа с базами данных
*/
package db

import (
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
	"github.com/alrusov/loadavg"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
	"github.com/alrusov/panic"
	"github.com/alrusov/stdhttp"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	// Список используемых баз данных. Ключ - имя соединения
	Config map[string]*DB

	// Описание базы
	DB struct {
		Name   string   `toml:"-"`
		Active bool     `toml:"active"`
		Driver string   `toml:"driver"`
		DSN    []string `toml:"dsn"`

		Queries misc.StringMap `toml:"queries"` // SQL запросы для этой базы, ключ - имя запроса

		conn []*sqlx.DB
	}

	PatternType int

	Stat struct {
		mutex  *sync.Mutex
		period time.Duration
		Stat   map[string]dbStat `json:"stat,omitempty"`
	}

	dbStat map[int]*connStat

	connStat struct {
		Query *funcStat `json:"query,omitempty"`
		Exec  *funcStat `json:"exec,omitempty"`
	}

	funcStat struct {
		InProgress      int64         `json:"inProgress,omitempty"`
		TotalRequests   int64         `json:"totalRequests,omitempty"`
		TotalDuration   time.Duration `json:"totalDuration,omitempty"`
		DurationAvg     int64         `json:"durationAvg,omitempty"`     // Load average по времени обработки
		LastRequestsAvg float64       `json:"lastRequestsAvg,omitempty"` // Load average по запросам
		LastDurationAvg int64         `json:"lastDurationAvg,omitempty"` // Load average по времени обработки

		requestsLA *loadavg.LoadAvg
		durationLA *loadavg.LoadAvg
	}

	SubstArg struct {
		Name  string
		Value any
	}
)

const (
	PatternNames = "@NAMES@"
	PatternVals  = "@VALS@"
	PatternPairs = "@PAIRS@"
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

	stat = &Stat{
		mutex:  new(sync.Mutex),
		period: time.Second * 60,
		Stat:   make(map[string]dbStat, 8),
	}
)

//----------------------------------------------------------------------------------------------------------------------------//

func init() {
	// Регистрируем инициализатор
	initializer.RegisterModuleInitializer(initModule)
}

// Инициализация
func initModule(appCfg any, h *stdhttp.HTTP) (err error) {
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
		Name:  name,
		Value: value,
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func SetStatPeriod(newPeriod time.Duration) (oldPeriod time.Duration) {
	oldPeriod, stat.period = stat.period, newPeriod
	return
}

func GetStat() *Stat {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	for _, d := range stat.Stat {
		for _, c := range d {
			c.Query.update()
			c.Exec.update()
		}
	}
	return stat.cloneVals()
}

//----------------------------------------------------------------------------------------------------------------------------//

func (src *Stat) cloneVals() (dst *Stat) {
	// уже залочено
	dst = &Stat{
		mutex:  nil,
		period: 0,
		Stat:   make(map[string]dbStat, len(src.Stat)),
	}

	for name, srcDB := range src.Stat {
		dstDB := make(dbStat, len(srcDB))
		for idx, srcConn := range srcDB {
			q := *srcConn.Query
			e := *srcConn.Exec
			dstDB[idx] = &connStat{
				Query: &q,
				Exec:  &e,
			}
		}
		dst.Stat[name] = dstDB
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func newStat(name string, idx int) (err error) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	db, exists := stat.Stat[name]
	if !exists {
		db = make(dbStat, 8)
	}

	if _, exists = db[idx]; exists {
		err = fmt.Errorf("stat for %s.%d already exists", name, idx)
		return
	}

	db[idx] = newConnStat()

	stat.Stat[name] = db

	return
}

func newConnStat() *connStat {
	return &connStat{
		Query: newFuncStat(),
		Exec:  newFuncStat(),
	}
}

func newFuncStat() *funcStat {
	return &funcStat{
		requestsLA: loadavg.Init(stat.period),
		durationLA: loadavg.Init(stat.period),
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) statBegin(idx int, isExec bool) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	c := db.getConnStat(idx)
	if c == nil {
		return
	}

	if isExec {
		c.Exec.begin()
	} else {
		c.Query.begin()
	}
}

func (db *DB) statEnd(idx int, isExec bool, duration time.Duration) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	c := db.getConnStat(idx)
	if c == nil {
		return
	}

	if isExec {
		c.Exec.end(duration)
	} else {
		c.Query.end(duration)
	}
}

func (db *DB) getConnStat(idx int) (c *connStat) {
	// уже залочено!

	d, exists := stat.Stat[db.Name]
	if !exists {
		return
	}

	c = d[idx]
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (f *funcStat) begin() {
	// уже залочено!

	f.InProgress++
	f.TotalRequests++
}

func (f *funcStat) end(duration time.Duration) {
	// уже залочено!

	f.InProgress--
	f.TotalDuration += duration

	f.requestsLA.Add(1)
	f.durationLA.Add(float64(duration))
}

func (f *funcStat) update() {
	// уже залочено!

	if f.TotalRequests > 0 {
		f.DurationAvg = int64(float64(f.TotalDuration) / float64(f.TotalRequests))
	}

	f.LastRequestsAvg = f.requestsLA.Value()
	f.LastDurationAvg = int64(f.durationLA.Value())
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

	if len(x.DSN) == 0 {
		msgs.Add("empty dsn")
	}

	for i, dsn := range x.DSN {
		if dsn == "" {
			msgs.Add("empty dsn[%d]", i)
		}
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) Connect(idx int) (conn *sqlx.DB, doRetry bool, err error) {

	// отрицательный idx оставим на будущее, например выбор произвольной базы

	if idx < 0 || idx >= int(len(db.DSN)) {
		err = fmt.Errorf("%s: illegal db index %d, expected from 0 to %d", db.Name, idx, len(db.DSN))
		return
	}

	doRetry = true

	conn, err = sqlx.Open(db.Driver, db.DSN[idx])
	if err != nil {
		err = fmt.Errorf("%s: %s", db.Name, err)
		return
	}

	err = conn.Ping()
	if err != nil {
		err = fmt.Errorf("%s: %s", db.Name, err)
		return
	}

	err = newStat(db.Name, idx)
	if err != nil {
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (c Config) ConnectAll() (err error) {
	msgs := misc.NewMessages()

	wg := new(sync.WaitGroup)
	for _, db := range c {
		wg.Add(len(db.DSN))
	}

	for _, db := range c {
		db.conn = make([]*sqlx.DB, len(db.DSN))

		for i := 0; i < len(db.DSN); i++ {
			db := db
			i := i

			go func() {
				panicID := panic.ID()
				defer panic.SaveStackToLogEx(panicID)

				defer wg.Done()

				for misc.AppStarted() {
					conn, doRetry, err := db.Connect(i)
					if err != nil {
						Log.Message(log.WARNING, "%s", err)
						if !doRetry {
							return
						}
						misc.Sleep(5 * time.Second)
						continue
					}

					db.conn[i] = conn
					dbName := db.Name

					Log.Message(log.INFO, "[%s] database connection created", dbName)

					misc.AddExitFunc(
						fmt.Sprintf("db.%s[%d]", dbName, i),
						func(_ int, _ any) {
							conn.Close()
							Log.Message(log.INFO, "[%s] database connection closed", dbName)
						},
						nil,
					)

					break
				}
			}()
		}
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

func GetConn(dbName string, idx int) (conn *sqlx.DB, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	if idx < 0 || idx >= len(db.DSN) {
		err = fmt.Errorf("%s: illegal db index %d, expected from 0 to %d", db.Name, idx, len(db.DSN))
		return
	}

	conn = db.conn[idx]

	if conn == nil {
		err = fmt.Errorf("database %s not connected", dbName)
		return

	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) Query(idx int, dest any, queryName string, fields []string, vars []any) (err error) {
	t0 := misc.NowUnixNano()

	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %s", queryName, err)
		}
	}()

	db.statBegin(idx, false)
	defer func() {
		db.statEnd(idx, false, time.Duration(misc.NowUnixNano()-t0))
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

	conn, preparedVars, q, err := db.prepareQuery(idx, queryName, vars)
	if err != nil {
		return
	}

	q = fillFields(q, PatternTypeSelect, 0, fields)

	switch dest := dest.(type) {
	default:
		if k := reflect.Indirect(reflect.ValueOf(dest)).Kind(); k == reflect.Slice {
			// Слайс [предположительно] структур - пытаемся залить данные туда
			err = conn.Select(dest, q, preparedVars...)
			return
		}

		err = fmt.Errorf(`illegal type of the dest (%T)`, dest)
		return

	case *([]misc.InterfaceMap):
		// Иначе - зальем в []InterfaceMap

		var rows *sqlx.Rows
		rows, err = conn.Queryx(q, preparedVars...)
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

func Query(dbName string, idx int, dest any, queryName string, fields []string, vars []any) (err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.Query(idx, dest, queryName, fields, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) ExecEx(idx int, queryName string, tp PatternType, startIdx int, fields []string, vars []any) (result sql.Result, err error) {
	t0 := misc.NowUnixNano()

	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %s", queryName, err)
		}
	}()

	db.statBegin(idx, true)
	defer func() {
		db.statEnd(idx, true, time.Duration(misc.NowUnixNano()-t0))
	}()

	id := atomic.AddUint64(&lastID, 1)
	logSrc := fmt.Sprintf("E.%s.%d", db.Name, id)

	if Log.CurrentLogLevel() >= log.TIME {
		defer misc.LogProcessingTime(Log.Name(), "", id, db.Name, "", t0)
		Log.MessageWithSource(log.TRACE2, logSrc, "%s %v", queryName, vars)
	}

	conn, preparedVars, q, err := db.prepareQuery(idx, queryName, vars)
	if err != nil {
		return
	}

	q = fillFields(q, tp, startIdx, fields)

	result, err = conn.Exec(q, preparedVars...)
	if err != nil {
		return
	}

	return
}

func ExecEx(dbName string, idx int, queryName string, tp PatternType, startIdx int, fields []string, vars []any) (result sql.Result, err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.ExecEx(idx, queryName, tp, startIdx, fields, vars)
}

func (db *DB) Exec(idx int, queryName string, vars []any) (result sql.Result, err error) {
	return db.ExecEx(idx, queryName, PatternTypeNone, 0, nil, vars)
}

func Exec(dbName string, idx int, queryName string, vars []any) (result sql.Result, err error) {
	return ExecEx(dbName, idx, queryName, PatternTypeNone, 0, nil, vars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) prepareQuery(idx int, queryName string, vars []any) (conn *sqlx.DB, preparedVars []any, q string, err error) {
	q, err = db.GetQuery(queryName)
	if err != nil {
		return
	}

	conn, err = GetConn(db.Name, idx)
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
		if reflect.ValueOf(v).Kind() == reflect.Slice {
			q, preparedVars, err = sqlx.In(q, preparedVars...)
			if err != nil {
				return
			}

			q = conn.Rebind(q)
			break
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
			s, err = misc.Iface2String(v.Value)
			if err != nil {
				err = fmt.Errorf(`subst %v: %s`, v, err)
				return
			}

			newQ = strings.ReplaceAll(newQ, "@"+v.Name+"@", s)
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func fillFields(q string, tp PatternType, startIdx int, fields []string) string {
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
			vv[i] = "$" + strconv.FormatInt(int64(startIdx+i), 10)
		}
		v := strings.Join(vv, ",")

		q = strings.ReplaceAll(q, PatternNames, f)
		q = strings.ReplaceAll(q, PatternVals, v)

	case PatternTypeUpdate:
		vv := make([]string, nf)
		for i := 0; i < nf; i++ {
			vv[i] = fmt.Sprintf("%s=$%d", fields[i], startIdx+i)
		}

		v := strings.Join(vv, ",")
		q = strings.ReplaceAll(q, PatternPairs, v)
	}

	return q
}

//----------------------------------------------------------------------------------------------------------------------------//

func FieldsList(o any) (list []string, err error) {
	t := reflect.TypeOf(o)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		err = fmt.Errorf("%T is not a struct", o)
		return
	}

	n := t.NumField()
	list = make([]string, 0, n)

	for i := 0; i < n; i++ {
		f := t.Field(i)

		if !f.IsExported() {
			continue
		}

		t := f.Type
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}

		name := misc.StructFieldName(&f, "db")

		if name == "-" {
			continue
		}

		if name != "" {
			field := name
			as := name

			defVal, ok := f.Tag.Lookup("default")
			if ok {
				v := reflect.New(t).Interface()
				err = misc.Iface2IfacePtr(defVal, v)
				if err == nil {
					v = reflect.ValueOf(v).Elem().Interface()
					if f.Type.Kind() == reflect.String {
						v = "'" + v.(string) + "'"
					}
					field = fmt.Sprintf("COALESCE(%s, %v)", name, v)
				}
			}

			s := fmt.Sprintf(`%s AS "%s"`, field, as)
			list = append(list, s)
			continue
		}

		if t.Kind() != reflect.Struct {
			continue
		}

		var subList []string
		subList, err = FieldsList(reflect.New(t).Interface())
		if err != nil {
			return
		}
		list = append(list, subList...)
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

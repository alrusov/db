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

	Stat struct {
		mutex  *sync.Mutex
		period time.Duration
		Stat   map[string]*connStat `json:"stat,omitempty"`
	}

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
		name  string
		value any
	}
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

	stat = &Stat{
		mutex:  new(sync.Mutex),
		period: time.Second * 60,
		Stat:   make(map[string]*connStat, 8),
	}
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

func SetStatPeriod(newPeriod time.Duration) (oldPeriod time.Duration) {
	oldPeriod, stat.period = stat.period, newPeriod
	return
}

func GetStat() *Stat {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	for _, s := range stat.Stat {
		s.Query.update()
		s.Exec.update()
	}
	return stat.cloneVals()
}

//----------------------------------------------------------------------------------------------------------------------------//

func (src *Stat) cloneVals() (dst *Stat) {
	// уже залочено
	dst = &Stat{
		mutex:  nil,
		period: 0,
		Stat:   make(map[string]*connStat, len(src.Stat)),
	}

	for name, srcConn := range src.Stat {
		q := *srcConn.Query
		e := *srcConn.Exec
		dst.Stat[name] = &connStat{
			Query: &q,
			Exec:  &e,
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func newStat(name string) (err error) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	_, exists := stat.Stat[name]
	if exists {
		err = fmt.Errorf(`stat for "%s" already exists`, name)
		return
	}

	stat.Stat[name] = newConnStat()

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

func (db *DB) statBegin(isExec bool) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	c := db.getConnStat()
	if c == nil {
		return
	}

	if isExec {
		c.Exec.begin()
	} else {
		c.Query.begin()
	}
}

func (db *DB) statEnd(isExec bool, duration time.Duration) {
	stat.mutex.Lock()
	defer stat.mutex.Unlock()

	c := db.getConnStat()
	if c == nil {
		return
	}

	if isExec {
		c.Exec.end(duration)
	} else {
		c.Query.end(duration)
	}
}

func (db *DB) getConnStat() (c *connStat) {
	// already locked!

	c, exists := stat.Stat[db.Name]
	if !exists {
		c = nil
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (f *funcStat) begin() {
	// already locked!

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

	conn, preparedVars, q, err := db.prepareQuery(queryName, vars)
	if err != nil {
		return
	}

	if len(fields) == 0 {
		fields = []string{"*"}
	}

	q = fillFields(q, PatternTypeSelect, 0, fields)

	return db.query(conn, dest, q, preparedVars)
}

//----------------------------------------------------------------------------------------------------------------------------//

func (db *DB) query(conn *sqlx.DB, dest any, q string, preparedVars []any) (err error) {
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

func Query(dbName string, dest any, queryName string, fields []string, vars []any) (err error) {
	db, err := GetDB(dbName)
	if err != nil {
		return
	}

	return db.Query(dest, queryName, fields, vars)
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

	conn, preparedVars, q, err := db.prepareQuery(queryName, vars)
	if err != nil {
		return
	}

	q = fillFields(q, tp, firstDataFieldIdx, fields)

	if dest == nil || reflect.ValueOf(dest).IsNil() {
		result, err = conn.Exec(q, preparedVars...)
	} else {
		err = db.query(conn, dest, q, preparedVars)
		result = nil
	}
	if err != nil {
		return
	}

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

func (db *DB) prepareQuery(queryName string, vars []any) (conn *sqlx.DB, preparedVars []any, q string, err error) {
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

			switch val := v.value.(type) {
			default:
				s, err = misc.Iface2String(val)
				if err != nil {
					err = fmt.Errorf(`subst %#v: %s`, v, err)
					return
				}

			case JbBuildFormats:
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

func fillFields(q string, tp PatternType, firstDataFieldIdx int, fields []string) string {
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

type (
	FieldsList struct {
		all     []string
		allSrc  []string
		regular []string
		jbFull  misc.StringMap // name -> type
		jbShort misc.StringMap // name -> type
	}

	JbBuildFormats []*JbBuildFormat
	JbBuildFormat  struct {
		Idx    int
		Format string
	}
)

func (fields *FieldsList) All() []string {
	return fields.all
}

func (fields *FieldsList) AllSrc() []string {
	return fields.allSrc
}

func (fields *FieldsList) AllStr() string {
	return strings.Join(fields.all, ",")
}

func (fields *FieldsList) Regular() []string {
	return fields.regular
}

func (fields *FieldsList) RegularStr() string {
	return strings.Join(fields.regular, ",")
}

func (fields *FieldsList) JbFull() misc.StringMap {
	return fields.jbFull
}

func (fields *FieldsList) JbShort() misc.StringMap {
	return fields.jbShort
}

func (fields *FieldsList) JbSelectStr() string {
	ln := len(fields.jbShort)
	if ln == 0 {
		return ""
	}

	list := make([]string, 0, ln)

	for k, v := range fields.jbShort {
		s := fmt.Sprintf("%s %s", k, v)
		list = append(list, s)
	}

	return strings.Join(list, ",")
}

func (fields *FieldsList) JbPrepareBuild(vars misc.InterfaceMap) (jb JbBuildFormats, names []string, vals []any) {
	ln := len(fields.all)
	if ln == 0 {
		return
	}

	jb = make(JbBuildFormats, 0, ln)
	names = make([]string, 0, len(vars))
	vals = make([]any, 0, len(vars))
	jbVals := make([]any, 0, len(vars))

	idx := 0

	for name, val := range vars {
		tp, exists := fields.jbFull[name]

		n := strings.Split(name, ".")
		if len(n) > 1 {
			name = n[len(n)-1]
		}

		if !exists {
			names = append(names, name)
			vals = append(vals, val)
			continue
		}

		jb = append(jb, &JbBuildFormat{
			Idx:    idx,
			Format: fmt.Sprintf("'%s',$%%d::%s", name, tp),
		})
		idx++
		jbVals = append(jbVals, val)
	}

	vals = append(vals, jbVals...)
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func MakeFieldsList(o any) (fields *FieldsList, err error) {
	t := reflect.TypeOf(o)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		err = fmt.Errorf("%T is not a struct", o)
		return
	}

	n := t.NumField()
	fields = &FieldsList{
		all:     make([]string, 0, n),
		allSrc:  make([]string, 0, n),
		regular: make([]string, 0, n),
		jbFull:  make(misc.StringMap, n),
		jbShort: make(misc.StringMap, n),
	}

	for i := 0; i < n; i++ {
		sf := t.Field(i)

		if !sf.IsExported() {
			continue
		}

		t := sf.Type
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}

		name := misc.StructFieldName(&sf, "db")

		if name == "-" {
			continue
		}

		if name != "" {
			field := name
			as := name

			defVal, ok := sf.Tag.Lookup("default")
			if ok {
				v := reflect.New(t).Interface()
				err = misc.Iface2IfacePtr(defVal, v)
				if err == nil {
					v = reflect.ValueOf(v).Elem().Interface()
					if sf.Type.Kind() == reflect.String {
						v = "'" + v.(string) + "'"
					}
					field = fmt.Sprintf("COALESCE(%s, %v)", name, v)
				}
			}

			s := fmt.Sprintf(`%s AS "%s"`, field, as)
			fields.all = append(fields.all, s)
			fields.allSrc = append(fields.allSrc, name)

			tags := misc.StructFieldOpts(&sf, "db")
			tp, ok := tags["jb"]
			if !ok {
				fields.regular = append(fields.regular, s)
			} else {
				if tp == "" {
					tp = dbTpOf(t.Kind())
				}
				if tp != "" {
					fields.jbFull[name] = tp

					n := strings.Split(name, ".")
					if len(n) > 1 {
						name = n[len(n)-1]
					}
					fields.jbShort[name] = tp
				}
			}
			continue
		}

		if t.Kind() != reflect.Struct {
			continue
		}

		var subFields *FieldsList
		subFields, err = MakeFieldsList(reflect.New(t).Interface())
		if err != nil {
			return
		}

		fields.all = append(fields.all, subFields.all...)
		fields.regular = append(fields.regular, subFields.regular...)

		for k, v := range subFields.jbShort {
			fields.jbShort[k] = v
		}

		for k, v := range subFields.jbFull {
			fields.jbFull[k] = v
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func dbTpOf(k reflect.Kind) string {
	switch k {
	default:
		return ""

	case reflect.Bool:
		return "bool"

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"

	case reflect.Float32, reflect.Float64:
		return "float"

	case reflect.String:
		return "varchar"
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

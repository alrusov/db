package db

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

// jb works with pgsql only!

type (
	FieldsList struct {
		allDbNames  []string // все поля ["o.id", "x.name"]
		allDbSelect []string // все поля ["o.id AS \"o.id\"", "COALESCE(x.name, '') AS \"x.name\""]
		jbFieldsStr string

		byName     map[string]*FieldInfo // "Name"->...
		byJsonName map[string]*FieldInfo // "name"->...
		byDbName   map[string]*FieldInfo // "x.name"->...
	}

	FieldInfo struct {
		Parent    *FieldInfo
		Type      reflect.Type // reflect.TypeOf("")
		Container string       // "config"
		FieldName string       // "Name"
		JsonName  string       // "name"
		DbName    string       // "x.name"
		DbSelect  string       // "COALESCE(x.name, '') AS \"x.name\""
		JbName    string       // "name"
		JbType    string       // "varchar"
		DefVal    any          // 12345
		Tags      misc.StringMap
	}

	JbPairs []*JbPair
	JbPair  struct {
		Idx       int
		Format    string
		FieldInfo *FieldInfo
	}
)

var (
	typeTime     = reflect.TypeOf(time.Time{})
	typeDuration = reflect.TypeOf(Duration(0))
)

//----------------------------------------------------------------------------------------------------------------------------//

func (d JbPairs) Len() int {
	return len(d)
}

func (d JbPairs) Less(i, j int) bool {
	cmp := strings.Compare(d[i].FieldInfo.Container, d[j].FieldInfo.Container)

	if cmp == 0 {
		return d[i].FieldInfo.FieldName < d[j].FieldInfo.FieldName
	}

	return cmp < 0
}

func (d JbPairs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

//----------------------------------------------------------------------------------------------------------------------------//

func (fields *FieldsList) AllDbNames() []string {
	return fields.allDbNames
}

func (fields *FieldsList) AllDbSelect() []string {
	return fields.allDbSelect
}

func (fields *FieldsList) JbFieldsStr() string {
	return fields.jbFieldsStr
}

func (fields *FieldsList) ByJsonName() map[string]*FieldInfo {
	return fields.byJsonName
}

//----------------------------------------------------------------------------------------------------------------------------//

// jbPairs - шаблоны пар jb (имя, поставляемая переменная) с индексом переменной (среди jb полей) и родительским объектом
// names   - имена выбираемых обычных полей
// rows    - строки, с данными, сначала в соответствии с names, потом jb
func (fields *FieldsList) Prepare(data []misc.InterfaceMap) (jbPairs JbPairs, names []string, rows Bulk) {
	ln := len(fields.allDbSelect)
	if ln == 0 {
		return
	}

	jbPairs = make(JbPairs, 0, ln)
	names = make([]string, ln) // потом обрежем
	fieldsInfo := make([]*FieldInfo, 0, ln)
	rows = make(Bulk, 0, len(data))
	rowsJb := make(Bulk, 0, len(data))

	knownNames := make(misc.IntMap, ln) // name -> idx

	currIdx := 0
	currIdxJb := 0

	for _, row := range data {
		vals := make([]any, ln)
		valsJb := make([]any, ln)

		for fullName, val := range row {
			fieldInfo, exists := fields.byDbName[fullName]
			if !exists {
				continue
			}

			isJb := fieldInfo.JbName != ""

			isNew := false

			// Получаем текущий индекс в соответствующем блоке

			idx, exists := knownNames[fullName]
			if !exists {
				isNew = true

				fieldsInfo = append(fieldsInfo, fieldInfo)

				idx = currIdx
				if isJb {
					idx = currIdxJb
					currIdxJb++
				} else {
					currIdx++
				}

				knownNames[fullName] = idx
			}

			// Преобразуем значение в правильный тип

			var v any
			switch fieldInfo.Type {
			default:
				v = reflect.New(fieldInfo.Type).Interface()
			case typeDuration:
				s := ""
				v = &s
			}

			e := misc.Iface2IfacePtr(val, v)
			if e == nil {
				val = v
			}

			if !isJb {
				// Обычное поле

				name := fullName
				nameParts := strings.Split(fullName, ".")
				if len(nameParts) > 1 {
					// Имя без префиксов
					name = nameParts[len(nameParts)-1]
				}

				if isNew {
					names[idx] = name
				}
				vals[idx] = val
				continue
			}

			// jb поле

			if isNew {
				pair := &JbPair{
					Idx:       idx,
					Format:    fmt.Sprintf("'%s',$%%d::%s", fieldInfo.JbName, fieldInfo.JbType),
					FieldInfo: fieldInfo,
				}
				jbPairs = append(jbPairs, pair)
			}

			valsJb[idx] = val
		}

		rows = append(rows, vals)
		rowsJb = append(rowsJb, valsJb)
	}

	names = names[0:currIdx]

	for i := range rows {
		rows[i] = rows[i][0:currIdx]
		rows[i] = append(rows[i], rowsJb[i][:currIdxJb]...)

		for j, v := range rows[i] {
			if v == nil {
				rows[i][j] = fieldsInfo[j].DefVal
			}
		}
	}

	sort.Sort(jbPairs)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func MakeFieldsList(o any) (fields *FieldsList, err error) {
	fields, err = makeFieldsList(nil, o, "", "")
	if err != nil {
		return
	}

	ln := len(fields.byName)
	if ln == 0 {
		return
	}

	fields.allDbNames = make([]string, 0, ln)
	fields.allDbSelect = make([]string, 0, ln)
	jbFields := make([]string, 0, ln)

	for _, f := range fields.byName {
		if f.DbName != "" {
			fields.allDbNames = append(fields.allDbNames, f.DbName)
			fields.allDbSelect = append(fields.allDbSelect, f.DbSelect)
		}

		if f.JbName != "" && (f.Parent == nil || f.Parent.JbType != "jsonb") {
			s := fmt.Sprintf("%s %s", f.JbName, f.JbType)
			jbFields = append(jbFields, s)
		}
	}

	fields.jbFieldsStr = strings.Join(jbFields, ",")

	return
}

func makeFieldsList(parent *FieldInfo, o any, path string, jPath string) (fields *FieldsList, err error) {
	if path != "" {
		path += "."
	}

	if jPath != "" {
		jPath += "."
	}

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
		byName:     make(map[string]*FieldInfo, n),
		byJsonName: make(map[string]*FieldInfo, n),
		byDbName:   make(map[string]*FieldInfo, n),
	}

	for i := 0; i < n; i++ {
		var fieldInfo *FieldInfo
		fieldInfo, err = func() (fieldInfo *FieldInfo, err error) {
			sf := t.Field(i)

			if !sf.IsExported() {
				return
			}

			t := sf.Type
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}

			name := misc.StructTagName(&sf, "db")
			if name == "-" {
				return
			}

			switch t {
			default:
				t = misc.BaseType(t)
			case typeDuration:
				// keep
			}

			fieldInfo = &FieldInfo{
				Parent:    parent,
				Type:      t,
				FieldName: path + sf.Name,
				JsonName:  jPath + misc.StructTagName(&sf, "json"),
				Tags:      misc.StructTagOpts(&sf, "db"),
			}

			if name != "" || len(fieldInfo.Tags) > 1 {
				if name != "" {
					field := name
					as := name
					fieldInfo.DbName = name

					var v any
					switch fieldInfo.Type {
					default:
						v = reflect.New(fieldInfo.Type).Interface()
					case typeDuration:
						s := ""
						v = &s
					}

					defVal, defValExists := sf.Tag.Lookup("default")
					if defValExists {
						err = misc.Iface2IfacePtr(defVal, v)
						if err != nil {
							err = fmt.Errorf("%s(default): %s", name, err)
							return
						}

						vv := reflect.ValueOf(v).Elem()
						v = vv.Interface()

						switch vv.Kind() {
						case reflect.String:
							v = fmt.Sprintf("'%s'", strings.Replace(v.(string), "'", "''", -1))
						case reflect.Struct:
							switch vv := v.(type) {
							case time.Time:
								v = fmt.Sprintf("'%s'", misc.Time2JSON(vv))
							}
						}

						field = fmt.Sprintf("COALESCE(%s, %v)", name, v)
					}

					fieldInfo.DbSelect = fmt.Sprintf(`%s AS "%s"`, field, as)
					fieldInfo.DefVal = v
				}

				if tp, ok := fieldInfo.Tags["jb"]; ok {
					if tp == "" {
						tp = dbTpOf(t)
					}

					if tp != "" {
						fieldInfo.JbType = tp

						if container, ok := fieldInfo.Tags["container"]; ok {
							fieldInfo.Container = container
						}

						fieldName, ok := fieldInfo.Tags["jbField"]
						if !ok {
							fieldName = name
						}

						if fieldName != "" {
							n := strings.Split(fieldName, ".")
							if len(n) > 1 {
								fieldName = n[len(n)-1]
							}
							fieldInfo.JbName = fieldName
						}
					}
				}
			}

			if t.Kind() != reflect.Struct {
				return
			}

			if t == typeTime {
				return
			}

			var subFields *FieldsList
			subFields, err = makeFieldsList(fieldInfo, reflect.New(t).Interface(), path+sf.Name, jPath+misc.StructTagName(&sf, "json"))
			if err != nil {
				return
			}

			for k, v := range subFields.byName {
				fields.byName[k] = v
			}

			for k, v := range subFields.byJsonName {
				fields.byJsonName[k] = v
			}

			for k, v := range subFields.byDbName {
				fields.byDbName[k] = v
			}

			return
		}()

		if err != nil {
			return
		}

		if fieldInfo != nil && fieldInfo.FieldName != "" {
			fields.byName[fieldInfo.FieldName] = fieldInfo
			if fieldInfo.JsonName != "" && fieldInfo.JsonName != "-" {
				fields.byJsonName[fieldInfo.JsonName] = fieldInfo
			}
			if fieldInfo.DbName != "" && fieldInfo.DbName != "-" {
				fields.byDbName[fieldInfo.DbName] = fieldInfo
			}
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func dbTpOf(t reflect.Type) string {
	switch reflect.New(t).Interface().(type) {
	case Duration, *Duration:
		return "varchar"

	case time.Time, *time.Time:
		return "timestamp"

	default:
		switch t.Kind() {
		default:
			return ""

		case reflect.Bool:
			return "bool"

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return "bigint"

		case reflect.Float32, reflect.Float64:
			return "float8"

		case reflect.String:
			return "varchar"
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

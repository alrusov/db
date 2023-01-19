package db

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	FieldsList struct {
		json       misc.StringMap          // jsonName -> struct field name {"id": "ID"}
		json2db    misc.StringMap          // jsonName -> db field name {"id": "o.id"}
		all        []string                // все поля ["o.id AS \"o.id\"", "COALESCE(x.name, '') AS \"x.name\""]
		allSrc     []string                // все поля ["o.id", "x.name"]
		regular    []string                // обычные поля ["o.id AS \"o.id\"", "COALESCE(o.description, '') AS \"o.description\""]
		jbFull     misc.StringMap          // name -> type {"x.name": "int"}
		jbShort    misc.StringMap          // name -> type {"name": "int"}
		defVals    misc.InterfaceMap       // {"o.id": 0, "x.name": ""}
		typeByDB   map[string]reflect.Type // {"o.id": reflect.TypeOf(0)}
		typeByJson map[string]reflect.Type // {"id": reflect.TypeOf(0)}
	}

	JbPairs []*JbPair
	JbPair  struct {
		Idx    int
		Format string
	}
)

//----------------------------------------------------------------------------------------------------------------------------//

func (fields *FieldsList) Json() misc.StringMap {
	return fields.json
}

func (fields *FieldsList) Json2db() misc.StringMap {
	return fields.json2db
}

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

func (fields *FieldsList) TypeByDB() map[string]reflect.Type {
	return fields.typeByDB
}

func (fields *FieldsList) TypeByJson() map[string]reflect.Type {
	return fields.typeByJson
}

//----------------------------------------------------------------------------------------------------------------------------//

// jbPairs - шаблоны пар jb (имя, поставляемая переменная) с индексом переменной (среди jb полей)
// names   - имена выбираемых обычных полей
// rows    - строки, с данными, сначала в соответствии с names, потом jb
func (fields *FieldsList) Prepare(data []misc.InterfaceMap) (jbPairs JbPairs, names []string, rows Bulk) {
	ln := len(fields.all)
	if ln == 0 {
		return
	}

	jbPairs = make(JbPairs, 0, ln)
	names = make([]string, ln) // потом обрежем
	rows = make(Bulk, 0, len(data))
	rowsJb := make(Bulk, 0, len(data))

	knownNames := make(misc.IntMap, ln) // name -> idx

	currIdx := 0
	currIdxJb := 0

	for _, row := range data {
		vals := make([]any, ln)
		valsJb := make([]any, ln)

		for fullName, val := range row {
			// Имя без прификса
			name := fullName
			nameParts := strings.Split(fullName, ".")
			if len(nameParts) > 1 {
				name = nameParts[len(nameParts)-1]
			}

			tp, isJb := fields.jbFull[fullName]

			isNew := false

			// Получаем текущий индекс в соответствующем блоке

			idx, exists := knownNames[fullName]
			if !exists {
				isNew = true

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

			v := reflect.New(fields.typeByDB[fullName]).Interface()
			switch v.(type) {
			case Duration, *Duration:
				s := ""
				v = &s
			default:
			}

			e := misc.Iface2IfacePtr(val, v)
			if e == nil {
				val = v
			}

			if !isJb {
				// Обычное поле
				names[idx] = name // каждый раз, но пусть так
				vals[idx] = val
				continue
			}

			// jb поле

			if isNew {
				jbPairs = append(jbPairs,
					&JbPair{
						Idx:    idx,
						Format: fmt.Sprintf("'%s',$%%d::%s", name, tp),
					},
				)
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
				if defVal, exists := fields.defVals[names[j]]; exists {
					rows[i][j] = defVal
				}
			}
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func MakeFieldsList(o any) (fields *FieldsList, err error) {
	return makeFieldsList(o, "", "")
}

func makeFieldsList(o any, path string, jPath string) (fields *FieldsList, err error) {
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
		json:       make(misc.StringMap, n),
		json2db:    make(misc.StringMap, n),
		all:        make([]string, 0, n),
		allSrc:     make([]string, 0, n),
		regular:    make([]string, 0, n),
		jbFull:     make(misc.StringMap, n),
		jbShort:    make(misc.StringMap, n),
		defVals:    make(misc.InterfaceMap, n),
		typeByDB:   make(map[string]reflect.Type, n),
		typeByJson: make(map[string]reflect.Type, n),
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

			jName := jPath + misc.StructFieldName(&sf, "json")
			fields.json[jName] = path + sf.Name
			fields.json2db[jName] = name
			fields.typeByDB[name] = misc.BaseType(t)
			fields.typeByJson[jName] = fields.typeByDB[name]

			v := reflect.New(fields.typeByDB[name]).Interface()
			switch v.(type) {
			case Duration, *Duration:
				s := ""
				v = &s
			default:
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

			fields.defVals[name] = v

			s := fmt.Sprintf(`%s AS "%s"`, field, as)
			fields.all = append(fields.all, s)
			fields.allSrc = append(fields.allSrc, name)

			tags := misc.StructFieldOpts(&sf, "db")
			tp, ok := tags["jb"]
			if !ok {
				fields.regular = append(fields.regular, s)
			} else {
				if tp == "" {
					tp = dbTpOf(t)
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
		subFields, err = makeFieldsList(reflect.New(t).Interface(), path+sf.Name, jPath+misc.StructFieldName(&sf, "json"))
		if err != nil {
			return
		}

		fields.all = append(fields.all, subFields.all...)
		fields.allSrc = append(fields.allSrc, subFields.allSrc...)
		fields.regular = append(fields.regular, subFields.regular...)

		for k, v := range subFields.json {
			fields.json[k] = v
		}

		for k, v := range subFields.json2db {
			fields.json2db[k] = v
		}

		for k, v := range subFields.jbShort {
			fields.jbShort[k] = v
		}

		for k, v := range subFields.jbFull {
			fields.jbFull[k] = v
		}

		for k, v := range subFields.defVals {
			fields.defVals[k] = v
		}

		for k, v := range subFields.typeByDB {
			fields.typeByDB[k] = v
		}

		for k, v := range subFields.typeByJson {
			fields.typeByJson[k] = v
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
		return "datetime"

	default:
		switch t.Kind() {
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
}

//----------------------------------------------------------------------------------------------------------------------------//

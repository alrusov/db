package db

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	FieldsList struct {
		json    misc.StringMap // jsonName -> struct field name
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

//----------------------------------------------------------------------------------------------------------------------------//

func (fields *FieldsList) Json() misc.StringMap {
	return fields.json
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
	return makeFieldsList(o, "")
}

func makeFieldsList(o any, path string) (fields *FieldsList, err error) {
	if path != "" {
		path += "."
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
		json:    make(misc.StringMap, n),
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
			fields.json[path+misc.StructFieldName(&sf, "json")] = sf.Name

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
		subFields, err = makeFieldsList(reflect.New(t).Interface(), path+misc.StructFieldName(&sf, "json"))
		if err != nil {
			return
		}

		fields.all = append(fields.all, subFields.all...)
		fields.allSrc = append(fields.allSrc, subFields.allSrc...)
		fields.regular = append(fields.regular, subFields.regular...)

		for k, v := range subFields.json {
			fields.json[k] = v
		}

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

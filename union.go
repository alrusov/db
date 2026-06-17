package db

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

//----------------------------------------------------------------------------------------------------------------------------//

var (
	unionRE = regexp.MustCompile(`(.*)@@UNION\s+\((\@\w+\@)\)\s+IN\s+\(@(\w+)@\)\s+{(.*)}(.*)`)
)

//----------------------------------------------------------------------------------------------------------------------------//

func PrepareUnion(q string, vars []any) (string, error) {
	q = strings.ReplaceAll(q, "\n", " ")
	m := unionRE.FindAllStringSubmatch(q, -1)
	if len(m) == 0 {
		return q, nil
	}

	sb := new(strings.Builder)
	for _, mm := range m {
		if len(mm) != 6 {
			continue
		}

		list, err := getUnionVar(mm[3], vars)
		if err != nil {
			return "", err
		}

		sb.WriteString(mm[1])
		for i, v := range list {
			s := strings.ReplaceAll(mm[4], mm[2], v)
			if i != 0 {
				sb.WriteString(" UNION ALL ")
			}
			sb.WriteString(s)
		}
		sb.WriteString(mm[5])
	}

	return sb.String(), nil
}

func getUnionVar(name string, vars []any) (list []string, err error) {
	for _, v := range vars {
		switch v := v.(type) {
		case *UnionArg:
			if v.name == name {
				vv := reflect.ValueOf(v.value)
				if vv.Kind() != reflect.Slice {
					err = fmt.Errorf("%s is %s, not a slice", name, vv.Kind().String())
					return
				}

				ln := vv.Len()
				list = make([]string, 0, ln)
				for i := range ln {
					e := vv.Index(i)
					if !e.IsValid() || !e.CanInterface() {
						err = fmt.Errorf("%s[%d] is not valid", name, i)
						return
					}

					list = append(list, fmt.Sprint(e.Interface()))
				}
			}
			return
		}
	}

	return nil, nil
}

//----------------------------------------------------------------------------------------------------------------------------//

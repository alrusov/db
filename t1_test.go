package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"testing"

	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

func Test1(t *testing.T) {
	// TODO
}

//----------------------------------------------------------------------------------------------------------------------------//

func TestFillPatterns(t *testing.T) {
	params := []struct {
		src      string
		tp       PatternType
		startIdx int
		fields   []string
		expected string
	}{
		{
			src:      fmt.Sprintf("INSERT INTO table(%s) VALUES(%s)", PatternNames, PatternVals),
			tp:       PatternTypeInsert,
			startIdx: 10,
			fields:   []string{"f1", "f2", "f3", "f4", "f5"},
			expected: "INSERT INTO table(f1,f2,f3,f4,f5) VALUES($10,$11,$12,$13,$14)",
		},
		{
			src:      fmt.Sprintf("INSERT INTO table(%s) VALUES(%s)", PatternNames, PatternVals),
			tp:       PatternTypeInsert,
			startIdx: 10,
			fields:   []string{},
			expected: "INSERT INTO table() VALUES()",
		},

		{
			src:      fmt.Sprintf("UPDATE table SET %s WHERE id=$1", PatternPairs),
			tp:       PatternTypeUpdate,
			startIdx: 10,
			fields:   []string{"f1", "f2", "f3", "f4", "f5"},
			expected: "UPDATE table SET f1=$10,f2=$11,f3=$12,f4=$13,f5=$14 WHERE id=$1",
		},
		{
			src:      fmt.Sprintf("UPDATE table SET %s WHERE id=$1", PatternPairs),
			tp:       PatternTypeUpdate,
			startIdx: 10,
			fields:   []string{},
			expected: "UPDATE table SET  WHERE id=$1",
		},

		{
			src:      "",
			tp:       PatternTypeNone,
			startIdx: 0,
			fields:   []string{},
			expected: "",
		},
		{
			src:      "",
			tp:       PatternTypeInsert,
			startIdx: 0,
			fields:   []string{},
			expected: "",
		},
		{
			src:      "",
			tp:       PatternTypeUpdate,
			startIdx: 0,
			fields:   []string{},
			expected: "",
		},

		{
			src:      "",
			tp:       PatternTypeNone,
			startIdx: 10,
			fields:   []string{"f1", "f2", "f3", "f4", "f5"},
			expected: "",
		},
		{
			src:      "",
			tp:       PatternTypeInsert,
			startIdx: 10,
			fields:   []string{"f1", "f2", "f3", "f4", "f5"},
			expected: "",
		},
		{
			src:      "",
			tp:       PatternTypeUpdate,
			startIdx: 10,
			fields:   []string{"f1", "f2", "f3", "f4", "f5"},
			expected: "",
		},
	}

	for i, p := range params {
		q := fillPatterns(p.src, p.tp, p.startIdx, p.fields)

		if q != p.expected {
			t.Errorf(`[%d] got "%s", expected "%s"`, i+1, q, p.expected)
		}
	}
}

/*
func TestClickHouse(t *testing.T) {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&database=model")
	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`)
	if err != nil {
		log.Fatal(err)
	}

	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	)
	defer stmt.Close()

	for i := 0; i < 1900000; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	n := 100
	for rows.Next() {
		n--
		if n == 0 {
			break
		}

		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
*/
//----------------------------------------------------------------------------------------------------------------------------//

func TestNull(t *testing.T) {
	src := []byte(`[{"i":0,"u":0,"f":1.234,"s":"1234567890","b":true,"t":"2022-06-22T12:13:14.789Z"},{"i":null,"u":null,"f":null,"s":null,"b":null,"t":null}]`)

	type d struct {
		I NullInt64   `json:"i"`
		U NullUint64  `json:"u"`
		F NullFloat64 `json:"f"`
		S NullString  `json:"s"`
		B NullBool    `json:"b"`
		T NullTime    `json:"t"`
	}

	var v []d

	err := json.Unmarshal(src, &v)
	if err != nil {
		t.Fatal(err)
	}

	dst, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(src, dst) {
		t.Fatalf("\ngot \"%s\"\nexp \"%s\"", dst, src)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func TestJbPairs2String(t *testing.T) {
	type (
		L1_1 struct {
			L1_V1 int    `db:"o.l1_1->>'v1_1_1',jb,jbField=v1_1_1,container=l1_1"`
			L1_V2 string `db:"o.l1_1->>'v1_1_2',jb,jbField=v1_1_2,container=l1_1"`
		}

		L1_2 struct {
			L2_V1 int    `db:"o.l1_2->>'v1_2_1',jb,jbField=v1_2_1,container=l1_2"`
			L2_V2 string `db:"o.l1_2->>'v1_2_2',jb,jbField=v1_2_2,container=l1_2"`
		}

		L0 struct {
			ID uint64 `db:"o.id"`
			X  uint64 `db:"x.x,jb"`
			V1 *L1_1  `db:",jb=jsonb,jbField=v1,container=o.j"`
			V2 *L1_2  `db:",jb=jsonb,jbField=v2,container=o.j"`
		}
	)

	fl, err := MakeFieldsList(&L0{})
	if err != nil {
		t.Fatal(err)
	}

	jbp, names, rows := fl.Prepare(
		[]misc.InterfaceMap{
			{"x.x": 222, "o.id": 123, "o.l1_1->>'v1_1_1'": 1, "o.l1_1->>'v1_1_2'": "qqq", "o.l1_2->>'v1_2_1'": 2, "o.l1_2->>'v1_2_2'": "www"},
		},
	)

	runtime.KeepAlive(jbp)
	runtime.KeepAlive(names)

	expectedNames := []string{"id"}
	if !reflect.DeepEqual(names, expectedNames) {
		t.Errorf("names: got %v, expected %v", names, expectedNames)
	}

	runtime.KeepAlive(rows)

	compareRE := regexp.MustCompile(`\$\d+`)
	equal := func(s1 string, s2 string) bool {
		s1 = compareRE.ReplaceAllString(s1, "#")
		s2 = compareRE.ReplaceAllString(s2, "#")

		return s1 == s2
	}

	maxFuncPairsCount = 1

	expected := "(jsonb_build_object('x',$4::bigint)||jsonb_build_object('l1_1',(jsonb_build_object('v1_1_1',$0::bigint)||jsonb_build_object('v1_1_2',$1::varchar)::jsonb))||jsonb_build_object('l1_2',(jsonb_build_object('v1_2_1',$2::bigint)||jsonb_build_object('v1_2_2',$3::varchar)::jsonb))::jsonb)"
	s := jbp.String(PatternTypeInsert)
	if !equal(s, expected) {
		t.Errorf("%d.insert: got\n%s\nexpected\n%s\n", maxFuncPairsCount, s, expected)
	}

	expected = "(jsonb_build_object('x',$4::bigint)||jsonb_build_object('l1_1',COALESCE(o.j->'l1_1', '{}'::jsonb)||(jsonb_build_object('v1_1_1',$0::bigint)||jsonb_build_object('v1_1_2',$1::varchar)::jsonb))||jsonb_build_object('l1_2',COALESCE(o.j->'l1_2', '{}'::jsonb)||(jsonb_build_object('v1_2_1',$2::bigint)||jsonb_build_object('v1_2_2',$3::varchar)::jsonb))::jsonb)"
	s = jbp.String(PatternTypeUpdate)
	if !equal(s, expected) {
		t.Errorf("%d.update: got\n%s\nexpected\n%s\n", maxFuncPairsCount, s, expected)
	}

	maxFuncPairsCount = 2

	expected = "(jsonb_build_object('x',$4::bigint,'l1_1',(jsonb_build_object('v1_1_1',$0::bigint,'v1_1_2',$1::varchar)::jsonb))||jsonb_build_object('l1_2',(jsonb_build_object('v1_2_1',$2::bigint,'v1_2_2',$3::varchar)::jsonb))::jsonb)"
	s = jbp.String(PatternTypeInsert)
	if !equal(s, expected) {
		t.Errorf("%d.insert: got\n%s\nexpected\n%s\n", maxFuncPairsCount, s, expected)
	}

	expected = "(jsonb_build_object('x',$4::bigint,'l1_1',COALESCE(o.j->'l1_1', '{}'::jsonb)||(jsonb_build_object('v1_1_1',$0::bigint,'v1_1_2',$1::varchar)::jsonb))||jsonb_build_object('l1_2',COALESCE(o.j->'l1_2', '{}'::jsonb)||(jsonb_build_object('v1_2_1',$2::bigint,'v1_2_2',$3::varchar)::jsonb))::jsonb)"
	s = jbp.String(PatternTypeUpdate)
	if !equal(s, expected) {
		t.Errorf("%d.update: got\n%s\nexpected\n%s\n", maxFuncPairsCount, s, expected)
	}

	maxFuncPairsCount = 50

	expected = "(jsonb_build_object('x',$4::bigint,'l1_1',(jsonb_build_object('v1_1_1',$0::bigint,'v1_1_2',$1::varchar)::jsonb),'l1_2',(jsonb_build_object('v1_2_1',$2::bigint,'v1_2_2',$3::varchar)::jsonb))::jsonb)"
	s = jbp.String(PatternTypeInsert)
	if !equal(s, expected) {
		t.Errorf("%d.insert: got\n%s\nexpected\n%s\n", maxFuncPairsCount, s, expected)
	}

	expected = "(jsonb_build_object('x',$4::bigint,'l1_1',COALESCE(o.j->'l1_1', '{}'::jsonb)||(jsonb_build_object('v1_1_1',$0::bigint,'v1_1_2',$1::varchar)::jsonb),'l1_2',COALESCE(o.j->'l1_2', '{}'::jsonb)||(jsonb_build_object('v1_2_1',$2::bigint,'v1_2_2',$3::varchar)::jsonb))::jsonb)"
	s = jbp.String(PatternTypeUpdate)
	if !equal(s, expected) {
		t.Errorf("%d.update: got\n%s\nexpected\n%s\n", maxFuncPairsCount, s, expected)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

/*
Работа с базами данных
*/
package db

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/alrusov/config"
	"github.com/alrusov/jsonw"
	"github.com/alrusov/misc"
)

var (
	nullS = "null"
	nullB = []byte(nullS)
)

//----------------------------------------------------------------------------------------------------------------------------//

func isNullS(s []byte) bool {
	return bytes.Equal(s, nullB)
}

//----------------------------------------------------------------------------------------------------------------------------//

type NullInt64 sql.NullInt64

func (v *NullInt64) Scan(value any) error {
	return (*sql.NullInt64)(v).Scan(value)
}

func (v NullInt64) Value() (driver.Value, error) {
	return sql.NullInt64(v).Value()
}

func (v NullInt64) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return nullB, nil
	}

	return jsonw.Marshal(v.Int64)
}

func (v *NullInt64) UnmarshalJSON(s []byte) error {
	if isNullS(s) {
		v.Int64 = 0
		v.Valid = false
		return nil
	}

	err := jsonw.Unmarshal(s, &v.Int64)
	v.Valid = err == nil
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

type NullUint64 struct {
	Valid  bool
	Uint64 uint64
}

func (v *NullUint64) Scan(value any) error {
	vv, err := misc.Iface2Uint(value)
	if err != nil {
		v.Uint64 = 0
		v.Valid = false
	}

	v.Uint64 = vv
	v.Valid = true
	return nil
}

func (v NullUint64) Value() (driver.Value, error) {
	// кривовато
	return sql.NullInt64{
		Int64: int64(v.Uint64),
		Valid: v.Valid,
	}.Value()
}

func (v NullUint64) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return nullB, nil
	}

	return []byte(strconv.FormatUint(v.Uint64, 10)), nil
}

func (v *NullUint64) UnmarshalJSON(s []byte) error {
	if isNullS(s) {
		v.Uint64 = 0
		v.Valid = false
		return nil
	}

	var err error
	v.Uint64, err = strconv.ParseUint(misc.UnsafeByteSlice2String(s), 10, 64)
	v.Valid = err == nil
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

type NullFloat64 sql.NullFloat64

func (v *NullFloat64) Scan(value any) error {
	return (*sql.NullFloat64)(v).Scan(value)
}

func (v NullFloat64) Value() (driver.Value, error) {
	return sql.NullFloat64(v).Value()
}

func (v NullFloat64) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return nullB, nil
	}

	return jsonw.Marshal(v.Float64)
}

func (v *NullFloat64) UnmarshalJSON(s []byte) error {
	if isNullS(s) {
		v.Float64 = 0
		v.Valid = false
		return nil
	}

	err := jsonw.Unmarshal(s, &v.Float64)
	v.Valid = err == nil
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

type NullString sql.NullString

func (v *NullString) Scan(value any) error {
	return (*sql.NullString)(v).Scan(value)
}

func (v NullString) Value() (driver.Value, error) {
	return sql.NullString(v).Value()
}

func (v NullString) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return nullB, nil
	}

	return jsonw.Marshal(v.String)
}

func (v *NullString) UnmarshalJSON(s []byte) error {
	if isNullS(s) {
		v.String = ""
		v.Valid = false
		return nil
	}

	err := json.Unmarshal(s, &v.String)
	v.Valid = err == nil
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

type NullBool sql.NullBool

func (v *NullBool) Scan(value any) error {
	return (*sql.NullBool)(v).Scan(value)
}

func (v NullBool) Value() (driver.Value, error) {
	return sql.NullBool(v).Value()
}

func (v NullBool) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return nullB, nil
	}

	return jsonw.Marshal(v.Bool)
}

func (v *NullBool) UnmarshalJSON(s []byte) error {
	if isNullS(s) {
		v.Bool = false
		v.Valid = false
		return nil
	}

	err := jsonw.Unmarshal(s, &v.Bool)
	v.Valid = err == nil
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

type NullTime sql.NullTime

func (v *NullTime) Scan(value any) error {
	return (*sql.NullTime)(v).Scan(value)
}

func (v NullTime) Value() (driver.Value, error) {
	return sql.NullTime(v).Value()
}

func (v NullTime) MarshalJSON() ([]byte, error) {
	if !v.Valid {
		return nullB, nil
	}

	return jsonw.Marshal(v.Time)
}

func (v *NullTime) UnmarshalJSON(s []byte) error {
	if isNullS(s) {
		v.Time = time.Time{}
		v.Valid = false
		return nil
	}

	err := json.Unmarshal(s, &v.Time)
	v.Valid = err == nil
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

type Duration config.Duration

func (v *Duration) Scan(value any) error {
	*v = 0

	if value == nil {
		return nil
	}

	s, err := misc.Iface2String(value)
	if err != nil {
		return err
	}

	var d config.Duration
	err = (&d).UnmarshalText(misc.UnsafeString2ByteSlice(s))
	if err != nil {
		return err
	}

	*v = Duration(d)
	return err
}

func (v Duration) Value() (driver.Value, error) {
	return v.MarshalJSON()
}

func (v Duration) MarshalJSON() ([]byte, error) {
	s, err := config.Duration(v).MarshalText()
	if err == nil {
		s = bytes.Join([][]byte{{'"'}, s, {'"'}}, []byte{})
	}

	return s, err
}

func (v *Duration) UnmarshalJSON(s []byte) error {
	*v = 0

	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return fmt.Errorf("illegal format")
	}

	var d config.Duration
	err := (&d).UnmarshalText(s[1 : len(s)-1])
	if err != nil {
		return err
	}

	*v = Duration(d)
	return nil
}

//----------------------------------------------------------------------------------------------------------------------------//

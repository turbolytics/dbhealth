package report

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
)

// filled is a section with every field set, and two of each list element.
// A field's value comes from its path, so one field no other holds, and a
// new wire field moves no other value in the golden file.
func filled() *wire.Database {
	var d wire.Database
	fill(reflect.ValueOf(&d).Elem(), "")
	// The word the slot's lost metric reads, on the second slot alone.
	d.Replication.Slots[1].WALStatus = slotLost
	return &d
}

func fill(v reflect.Value, path string) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	n := int(h.Sum32() % 1_000_000)
	if v.Type() == reflect.TypeOf(time.Time{}) {
		v.Set(reflect.ValueOf(time.Date(2026, 1, 1, 0, 0, n, 0, time.UTC)))
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(v.Elem(), path)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			fill(v.Field(i), path+"."+jsonName(v.Type().Field(i)))
		}
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 2, 2)
		for i := 0; i < s.Len(); i++ {
			fill(s.Index(i), fmt.Sprintf("%s[%d]", path, i))
		}
		v.Set(s)
	case reflect.String:
		v.SetString(fmt.Sprintf("s%d", n))
	case reflect.Int, reflect.Int64:
		v.SetInt(int64(1000 + n))
	case reflect.Float64:
		v.SetFloat(float64(n) + 0.5)
	case reflect.Bool:
		v.SetBool(n%2 == 0)
	default:
		panic(fmt.Sprintf("fill has no case for %s", v.Kind()))
	}
}

// jsonName is a struct field's name on the wire.
func jsonName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	return name
}

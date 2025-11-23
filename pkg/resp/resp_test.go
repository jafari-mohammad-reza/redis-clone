// resp/resp_test.go
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"simple string", "OK", "+OK\r\n"},
		{"error", errors.New("ERR boom"), "-ERR boom\r\n"},
		{"integer", 12345, ":12345\r\n"},
		{"negative int", int64(-999), ":-999\r\n"},
		{"nil", nil, "$-1\r\n"},
		{"empty bulk", []byte{}, "$0\r\n\r\n"},
		{"bulk string", []byte("hello"), "$5\r\nhello\r\n"},
		{"array empty", []any{}, "*0\r\n"},
		{"array simple", []any{"GET", "key"}, "*2\r\n+GET\r\n+key\r\n"},
		{"array with nil", []any{"SET", "key", nil}, "*3\r\n+SET\r\n+key\r\n$-1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Marshal(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, []byte(tt.want)) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUnmarshalOne(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{"simple string", "+OK\r\n", Value{Typ: "string", Str: "OK"}},
		{"error", "-ERR test\r\n", Value{Typ: "error", Str: "ERR test"}},
		{"integer", ":123\r\n", Value{Typ: "integer", Num: 123}},
		{"null", "$-1\r\n", Value{Typ: "null"}},
		{"empty bulk", "$0\r\n\r\n", Value{Typ: "bulk", Bulk: ""}},
		{"bulk", "$5\r\nhello\r\n", Value{Typ: "bulk", Bulk: "hello"}},
		{"empty array", "*0\r\n", Value{Typ: "array", Array: []Value{}}},
		{"array", "*2\r\n+GET\r\n+key\r\n", Value{Typ: "array", Array: []Value{
			{Typ: "string", Str: "GET"},
			{Typ: "string", Str: "key"},
		}}},
		{"array with nil", "*3\r\n+SET\r\n+mykey\r\n$-1\r\n", Value{Typ: "array", Array: []Value{
			{Typ: "string", Str: "SET"},
			{Typ: "string", Str: "mykey"},
			{Typ: "null"},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(bytes.NewReader([]byte(tt.input)))
			got, err := UnmarshalOne(r)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWriteValue(t *testing.T) {
	tests := []struct {
		v    Value
		want string
	}{
		{Value{Typ: "string", Str: "OK"}, "+OK\r\n"},
		{Value{Typ: "error", Str: "ERR"}, "-ERR\r\n"},
		{Value{Typ: "integer", Num: 123}, ":123\r\n"},
		{Value{Typ: "null"}, "$-1\r\n"},
		{Value{Typ: "bulk", Bulk: ""}, "$-1\r\n"},
		{Value{Typ: "bulk", Bulk: "hello"}, "$5\r\nhello\r\n"},
		{Value{Typ: "array", Array: []Value{{Typ: "string", Str: "PING"}}}, "+PING\r\n"},
		{Value{Typ: "array", Array: []Value{
			{Typ: "bulk", Bulk: "GET"},
			{Typ: "bulk", Bulk: "key"},
		}}, "$3\r\nGET\r\n$3\r\nkey\r\n"},
		{Value{Typ: "array", Array: nil}, "*-1\r\n"},
	}

	for i, tt := range tests {
		var buf bytes.Buffer
		if err := WriteValue(&buf, tt.v); err != nil {
			t.Fatal(err)
		}
		if got := buf.String(); got != tt.want {
			t.Errorf("[%d] WriteValue(%+v)\n got: %q\nwant: %q", i, tt.v, got, tt.want)
		}
	}
}

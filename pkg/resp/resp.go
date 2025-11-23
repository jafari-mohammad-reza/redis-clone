package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type Value struct {
	Typ   string // "string", "error", "integer", "bulk", "array", "null"
	Str   string
	Num   int64
	Bulk  string
	Array []Value
}

func Marshal(v any) ([]byte, error) {
	switch v := v.(type) {
	case string:
		return []byte("+" + v + "\r\n"), nil
	case error:
		return []byte("-" + v.Error() + "\r\n"), nil
	case int, int8, int16, int32, int64:
		return []byte(":" + strconv.FormatInt(reflectValueToInt64(v), 10) + "\r\n"), nil
	case nil:
		return []byte("$-1\r\n"), nil
	case []byte:
		return []byte("$" + strconv.Itoa(len(v)) + "\r\n" + string(v) + "\r\n"), nil
	case []any:
		b := []byte("*" + strconv.Itoa(len(v)) + "\r\n")
		for _, item := range v {
			bb, err := Marshal(item)
			if err != nil {
				return nil, err
			}
			b = append(b, bb...)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// Simple helper for int conversion
func reflectValueToInt64(v any) int64 {
	switch i := v.(type) {
	case int:
		return int64(i)
	case int64:
		return i
	case int32:
		return int64(i)
	default:
		return 0
	}
}

// UnmarshalOne reads exactly ONE complete RESP value from r
func UnmarshalOne(r *bufio.Reader) (Value, error) {
	b, err := r.Peek(1)
	if err != nil {
		if err == io.EOF {
			return Value{}, io.EOF
		}
		return Value{}, err
	}

	// If it's not a valid RESP prefix, read the whole line as error/plaintext
	if len(b) == 0 || (b[0] != '+' && b[0] != '-' && b[0] != ':' && b[0] != '$' && b[0] != '*') {
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Typ: "error", Str: "Server sent: " + line}, nil
	}
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, errors.New("empty line")
	}

	switch line[0] {
	case '+': // Simple String
		return Value{Typ: "string", Str: string(line[1:])}, nil
	case '-': // Error
		return Value{Typ: "error", Str: string(line[1:])}, nil
	case ':': // Integer
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		return Value{Typ: "integer", Num: n}, err
	case '$': // Bulk String
		if line == "$-1" {
			return Value{Typ: "null"}, nil
		}
		length, _ := strconv.Atoi(string(line[1:]))
		if length < 0 {
			return Value{}, errors.New("negative bulk length")
		}
		buf := make([]byte, length+2) // +2 for \r\n
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return Value{}, err
		}
		return Value{Typ: "bulk", Bulk: string(buf[:length])}, nil
	case '*': // Array
		if line == "*-1" {
			return Value{Typ: "null"}, nil
		}
		count, _ := strconv.Atoi(string(line[1:]))
		if count < 0 {
			return Value{}, errors.New("negative array length")
		}
		arr := make([]Value, count)
		for i := 0; i < count; i++ {
			val, err := UnmarshalOne(r)
			if err != nil {
				return Value{}, err
			}
			arr[i] = val
		}
		return Value{Typ: "array", Array: arr}, nil
	default:
		return Value{}, fmt.Errorf("unexpected prefix: %c", line[0])
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1] // remove trailing \n
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1] // remove \r if present
		}
		return line, nil
	}
	return "", errors.New("invalid line ending")
}

// WriteValue writes a Value directly to a writer (useful for servers)
func WriteValue(w io.Writer, v Value) error {
	var data []byte
	switch v.Typ {
	case "string":
		data = []byte("+" + v.Str + "\r\n")
	case "error":
		data = []byte("-" + v.Str + "\r\n")
	case "integer":
		data = []byte(":" + strconv.FormatInt(v.Num, 10) + "\r\n")
	case "bulk":
		if v.Bulk == "" {
			data = []byte("$-1\r\n")
		} else {
			data = []byte("$" + strconv.Itoa(len(v.Bulk)) + "\r\n" + v.Bulk + "\r\n")
		}
	case "null":
		data = []byte("$-1\r\n")
	case "array":
		if v.Array == nil {
			data = []byte("*-1\r\n")
		} else {
			data = []byte("*" + strconv.Itoa(len(v.Array)) + "\r\n")
			for _, item := range v.Array {
				if err := WriteValue(w, item); err != nil {
					return err
				}
			}
			return nil
		}
	default:
		return errors.New("unknown type")
	}
	_, err := w.Write(data)
	return err
}

package storage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ValueType int8

const (
	TypeString ValueType = iota
	TypeList
)

type Value struct {
	Type   ValueType
	String string
	List   []string
	Expiry time.Time
}

type Entry struct {
	Value Value
}

type Database struct {
	data map[string]Entry
	mu   sync.RWMutex
}

type Storage struct {
	databases map[int]*Database
	mu        sync.RWMutex
}

func NewStorage() *Storage {
	databases := make(map[int]*Database, 10)
	for i := 0; i < 10; i++ {
		databases[i] = &Database{
			data: make(map[string]Entry),
		}
	}
	return &Storage{
		databases: databases,
	}
}

func (s *Storage) Set(key, val string, exp time.Duration, db int) error {
	if db >= 10 {
		return fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].Set(key, val, exp)
}

func (d *Database) Set(key, val string, exp time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	expiry := time.Time{}
	if exp > 0 {
		expiry = time.Now().Add(exp)
	}

	d.data[key] = Entry{
		Value: Value{
			Type:   TypeString,
			String: val,
			Expiry: expiry,
		},
	}
	return nil
}

func (s *Storage) Get(key string, db int) (*Entry, error) {
	if db >= 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].Get(key), nil
}

func (d *Database) Get(key string) *Entry {
	d.mu.RLock()
	entry, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		return nil
	}

	if !entry.Value.Expiry.IsZero() && time.Now().After(entry.Value.Expiry) {
		d.mu.Lock()
		delete(d.data, key)
		d.mu.Unlock()
		return nil
	}

	return &entry
}

func (s *Storage) Del(key string, db int) int {
	if db >= 10 {
		return 0
	}
	return s.databases[db].Del(key)
}

func (d *Database) Del(key string) int {
	d.mu.RLock()
	_, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		return 0
	}
	d.mu.Lock()
	delete(d.data, key)
	d.mu.Unlock()
	return 1
}

func (s *Storage) Flush() error {
	s.mu.RLock()
	dbs := make([]*Database, 0, len(s.databases))
	for _, db := range s.databases {
		dbs = append(dbs, db)
	}
	s.mu.RUnlock()

	for _, db := range dbs {
		db.mu.Lock()
		db.data = make(map[string]Entry)
		db.mu.Unlock()
	}
	return nil
}

func (s *Storage) RPush(key string, items []string, db int) (int, error) {
	if db >= 10 {
		return 0, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].RPush(key, items)
}

func (d *Database) RPush(key string, items []string) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, exists := d.data[key]
	if !exists || entry.Value.Type != TypeList {
		d.data[key] = Entry{
			Value: Value{
				Type: TypeList,
				List: make([]string, 0),
			},
		}
		entry = d.data[key]
	}

	entry.Value.List = append(entry.Value.List, items...)
	d.data[key] = entry
	return len(entry.Value.List), nil
}

func (s *Storage) RLen(key string, db int) (int, error) {
	if db >= 10 {
		return 0, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].RLen(key)
}

func (d *Database) RLen(key string) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, ok := d.data[key]
	if !ok || entry.Value.Type != TypeList {
		return 0, nil
	}
	return len(entry.Value.List), nil
}

func (s *Storage) RRange(key string, from, to string, db int) (string, error) {
	if db >= 10 {
		return "", fmt.Errorf("invalid database %d", db)
	}
	fromInt, err := strconv.Atoi(from)
	if err != nil {
		return "", fmt.Errorf("invalid %d as from range", db)
	}
	toInt, err := strconv.Atoi(to)
	if err != nil {
		return "", fmt.Errorf("invalid %d as to range", db)
	}
	return s.databases[db].RRange(key, fromInt, toInt)
}

func (d *Database) RRange(key string, from, to int) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, ok := d.data[key]
	if !ok || entry.Value.Type != TypeList {
		return "", nil
	}

	list := entry.Value.List
	n := len(list)

	if from < 0 {
		from += n
	}
	if to < 0 {
		to += n
	}

	if from < 0 {
		from = 0
	}
	if to >= n {
		to = n - 1
	}
	if from > to {
		return "", nil
	}

	return strings.Join(list[from:to+1], ","), nil
}

func (s *Storage) LPush(key string, items []string, db int) (int, error) {
	if db >= 10 {
		return 0, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].LPush(key, items)
}
func (d *Database) LPush(key string, items []string) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, exists := d.data[key]
	if !exists || entry.Value.Type != TypeList {
		entry = Entry{
			Value: Value{
				Type: TypeList,
				List: []string{},
			},
		}
	}

	entry.Value.List = append(items, entry.Value.List...)

	d.data[key] = entry
	return len(entry.Value.List), nil
}

func (s *Storage) LRange(key string, from, to string, db int) (string, error) {
	if db >= 10 {
		return "", fmt.Errorf("invalid database %d", db)
	}
	fromInt, err := strconv.Atoi(from)
	if err != nil {
		return "", fmt.Errorf("invalid %d as from range", db)
	}
	toInt, err := strconv.Atoi(to)
	if err != nil {
		return "", fmt.Errorf("invalid %d as to range", db)
	}
	return s.databases[db].LRange(key, fromInt, toInt)
}

func (d *Database) LRange(key string, from, to int) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, ok := d.data[key]
	if !ok || entry.Value.Type != TypeList {
		return "", nil
	}

	list := entry.Value.List
	n := len(list)
	if n == 0 {
		return "", nil
	}

	if from < 0 {
		from += n
	}
	if to < 0 {
		to += n
	}

	if from < 0 {
		from = 0
	}
	if to >= n {
		to = n - 1
	}
	if from > to {
		return "", nil
	}

	return strings.Join(list[from:to+1], ","), nil
}

// TODO: add lpop and rpop
func (s *Storage) LPOP(key string, from, to string, db int) (string, error) {
	if db >= 10 {
		return "", fmt.Errorf("invalid database %d", db)
	}
	fromInt, err := strconv.Atoi(from)
	if err != nil {
		return "", fmt.Errorf("invalid %d as from range", db)
	}
	toInt, err := strconv.Atoi(to)
	if err != nil {
		return "", fmt.Errorf("invalid %d as to range", db)
	}
	return s.databases[db].LPOP(key, fromInt, toInt)
}

func (d *Database) LPOP(key string, from, to int) (string, error) {
	return "", nil
}

func (s *Storage) RPOP(key string, from, to string, db int) (string, error) {
	if db >= 10 {
		return "", fmt.Errorf("invalid database %d", db)
	}
	fromInt, err := strconv.Atoi(from)
	if err != nil {
		return "", fmt.Errorf("invalid %d as from range", db)
	}
	toInt, err := strconv.Atoi(to)
	if err != nil {
		return "", fmt.Errorf("invalid %d as to range", db)
	}
	return s.databases[db].RPOP(key, fromInt, toInt)
}

func (d *Database) RPOP(key string, from, to int) (string, error) {
	return "", nil
}

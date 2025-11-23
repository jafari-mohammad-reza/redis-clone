package storage

import (
	"fmt"
	"sync"
	"time"
)

type Storage struct {
	// TODO:  lru can be implemented to return most used keys without search in own database
	// we use pointer to database to be able to use their receiver functions
	databases map[int]*Database
	mu        sync.RWMutex
}

type Database struct {
	data map[string]Entry
	mu   sync.RWMutex
}
type Entry struct {
	Value Value
	Exp   time.Time
}
type ValueType int8

const (
	TypeString ValueType = iota
	TypeList
)

type Value struct {
	Type   ValueType
	String string
	List   []string
}

func NewStorage() *Storage {
	databases := make(map[int]*Database, 10)
	for i := range 10 {
		databases[i] = &Database{
			data: make(map[string]Entry),
			mu:   sync.RWMutex{},
		}
		go databases[i].Exp()
	}
	return &Storage{
		databases: databases,
		mu:        sync.RWMutex{},
	}
}

func (s *Storage) Flush() error {
	for _, db := range s.databases {
		if err := db.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) Flush() error {
	d.mu.Lock()
	d.data = make(map[string]Entry) // recreate database
	d.mu.Unlock()
	return nil
}

func (s *Storage) Set(key, val string, exp time.Duration, db int) error {

	if db > 10 {
		return fmt.Errorf("invalid database %d", db)
	}
	if err := s.databases[db].Set(key, val, exp); err != nil {
		return fmt.Errorf("failed to set %s in db %d: %s", key, db, err.Error())
	}
	return nil
}
func (d *Database) Set(key, val string, exp time.Duration) error {
	d.mu.Lock()
	d.data[key] = Entry{
		Value: Value{Type: TypeString, String: val},
		Exp:   time.Now().Add(exp),
	}
	d.mu.Unlock()
	return nil
}

func (s *Storage) Get(key string, db int) (*Entry, error) {
	if db > 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	val := s.databases[db].Get(key)

	return val, nil
}

func (d *Database) Get(key string) *Entry {
	d.mu.RLock()
	entry, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		return nil
	}
	if !entry.Exp.IsZero() && entry.Exp.Before(time.Now()) { // passive expiry
		// it means this is expired
		delete(d.data, key)
		return nil
	}
	return &entry
}

func (s *Storage) Del(key string, db int) int {

	if db > 10 {
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
	delete(d.data, key)
	return 1
}

func (d *Database) Exp() {
	tk := time.NewTicker(time.Minute)
	defer tk.Stop()
	for range tk.C {
		for key, entry := range d.data {
			if !entry.Exp.IsZero() && entry.Exp.Before(time.Now()) {
				d.mu.RLock()
				delete(d.data, key)
				d.mu.RUnlock()
			}
		}
	}
}

func (s *Storage) RPush(key string, items []string, db int) (int, error) {

	if db > 10 {
		return 0, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].RPush(key, items)
}
func (d *Database) RPush(key string, items []string) (int, error) {
	d.mu.RLock()
	exist, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		d.data[key] = Entry{
			Value: Value{
				Type: TypeList,
				List: items,
			},
		}
		return len(items), nil
	}

	for _, item := range items {
		exist.Value.List = append(exist.Value.List, item)
	}
	d.mu.Lock()
	d.data[key] = exist
	d.mu.Unlock()
	return len(exist.Value.List), nil
}

func (s *Storage) RLen(key string, db int) (int, error) {
	if db > 10 {
		return 0, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].RLen(key)
}

func (d *Database) RLen(key string) (int, error) {
	d.mu.RLock()
	list, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	if list.Value.Type != TypeList {
		return 0, nil
	}
	return len(list.Value.List), nil
}

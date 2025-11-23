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
	Value string
	Exp   time.Time
}

func NewStorage() *Storage {
	databases := make(map[int]*Database, 10)
	for i := range 10 {
		databases[i] = &Database{
			data: make(map[string]Entry),
			mu:   sync.RWMutex{},
		}
	}
	return &Storage{
		databases: databases,
		mu:        sync.RWMutex{},
	}
}

func (s *Storage) Set(key, val string, exp time.Duration, db int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	defer d.mu.Unlock()
	d.data[key] = Entry{
		Value: val,
		Exp:   time.Now().Add(exp),
	}
	return nil
}

func (s *Storage) Get(key string, db int) (*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if db > 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	val := s.databases[db].Get(key)
	return val, nil
}

func (d *Database) Get(key string) *Entry {
	entry, ok := d.data[key]
	if !ok {
		return nil
	}
	if entry.Exp.Before(time.Now()) {
		// it means this is expired
		delete(d.data, key)
		return nil
	}
	return &entry
}

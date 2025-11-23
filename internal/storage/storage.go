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
		go databases[i].Exp()
	}
	return &Storage{
		databases: databases,
		mu:        sync.RWMutex{},
	}
}

func (s *Storage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, db := range s.databases {
		if err := db.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data = make(map[string]Entry) // recreate database
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
	defer d.mu.Unlock()
	d.data[key] = Entry{
		Value: val,
		Exp:   time.Now().Add(exp),
	}
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
	defer d.mu.RUnlock()
	entry, ok := d.data[key]
	if !ok {
		return nil
	}
	if entry.Exp.Before(time.Now()) { // passive expiry
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
	defer d.mu.RUnlock()
	_, ok := d.data[key]
	if !ok {
		return 0
	}
	delete(d.data, key)
	return 1
}

func (d *Database) Exp() {
	tk := time.NewTicker(time.Second * 10)
	defer tk.Stop()
	for range tk.C {
		d.mu.Lock()
		defer d.mu.Unlock()
		for key, entry := range d.data {
			if entry.Exp.Before(time.Now()) {
				delete(d.data, key)
			}
		}
	}
}

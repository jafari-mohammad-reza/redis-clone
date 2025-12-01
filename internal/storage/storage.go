package storage

import (
	"errors"
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
	TypeStream
)

type Value struct {
	Type    ValueType
	String  string
	List    []string
	Streams []Stream
	Expiry  time.Time
}
type Stream struct {
	Key     string
	ID      string
	Entries [][2]string
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
func (s *Storage) LPOP(key string, count, db int) ([]string, error) {
	if db >= 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].LPOP(key, count)
}

func (d *Database) LPOP(key string, count int) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, exists := d.data[key]
	if !exists || entry.Value.Type != TypeList {
		return nil, nil
	}

	list := entry.Value.List
	n := len(list)
	if n == 0 {
		return nil, nil
	}

	if count < 0 || count > n {
		count = n
	}

	result := make([]string, count)

	if count == 0 {
		result = []string{list[0]}
	} else {
		copy(result, list[:count])
	}

	entry.Value.List = list[count:]
	d.data[key] = entry

	if len(entry.Value.List) == 0 {
		delete(d.data, key)
	}

	return result, nil
}

func (s *Storage) RPOP(key string, count, db int) ([]string, error) {
	if db >= 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].RPOP(key, count)
}

func (d *Database) RPOP(key string, count int) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, exists := d.data[key]
	if !exists || entry.Value.Type != TypeList {
		return nil, nil
	}

	list := entry.Value.List
	n := len(list)
	if n == 0 {
		return nil, nil
	}

	if count < 0 || count > n {
		count = n
	}

	start := n - count
	result := make([]string, count)
	copy(result, list[start:])

	entry.Value.List = list[:start]
	d.data[key] = entry

	if len(entry.Value.List) == 0 {
		delete(d.data, key)
	}

	return result, nil
}

func (s *Storage) BLPOP(key string, count, timeoutSec, db int) ([]string, error) {
	if db >= 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].BLPOP(key, count, timeoutSec)
}

func (d *Database) BLPOP(key string, count, timeoutSec int) ([]string, error) {
	if count <= 0 {
		count = 1
	}

	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	if timeoutSec == 0 {
		deadline = time.Time{}
	}

	for {
		d.mu.RLock()
		entry, exists := d.data[key]
		hasItems := exists && entry.Value.Type == TypeList && len(entry.Value.List) >= count
		d.mu.RUnlock()

		if hasItems {
			return d.LPOP(key, count)
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil, nil
		}

		time.Sleep(50 * time.Millisecond)
	}
}
func (s *Storage) BRPOP(key string, count, timeoutSec, db int) ([]string, error) {
	if db >= 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}
	return s.databases[db].BRPOP(key, count, timeoutSec)
}

func (d *Database) BRPOP(key string, count, timeoutSec int) ([]string, error) {
	if count <= 0 {
		count = 1
	}

	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	if timeoutSec == 0 {
		deadline = time.Time{}
	}

	for {
		d.mu.RLock()
		entry, exists := d.data[key]
		hasItems := exists && entry.Value.Type == TypeList && len(entry.Value.List) >= count
		d.mu.RUnlock()

		if hasItems {
			return d.RPOP(key, count)
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil, nil
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (s *Storage) TypeCmd(key string, db int) (*ValueType, error) {
	return s.databases[db].TypeCmd(key)
}

func (d *Database) TypeCmd(key string) (*ValueType, error) {
	d.mu.RLock()
	item, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		return nil, errors.New("key does not exists")
	}
	return &item.Value.Type, nil
}

func (s *Storage) XAdd(key, ID string, pairs [][2]string, db int) error {
	return s.databases[db].XAdd(key, ID, pairs)
}

func (d *Database) XAdd(key, ID string, pairs [][2]string) error {
	/*
		The ID must be strictly greater than the last entry's ID.
		The millisecondsTime portion of the new ID must be greater than or equal to the last entry's millisecondsTime.
		If the millisecondsTime values are equal, the sequenceNumber of the new ID must be greater than the last entry's sequenceNumber.
	*/
	item, ok := d.data[key]
	if ID == "" {
		// id is created by milisecond time stamp + - + sequence number
		// first find last sequence
		if !ok || len(item.Value.Streams) == 0 {
			// sequence is 0
			ID = fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0)
		} else {
			ID = fmt.Sprintf("%d-%d", time.Now().UnixMilli(), len(item.Value.Streams)-1)
		}
	} else {
		// validate ID
		if ok && len(item.Value.Streams) > 0 {
			lastStream := item.Value.Streams[len(item.Value.Streams)-1]
			lastParts := strings.Split(lastStream.ID, "-")
			newParts := strings.Split(ID, "-")
			if len(lastParts) != 2 || len(newParts) != 2 {
				return errors.New("invalid ID format")
			}
			lastMs, err := strconv.ParseInt(lastParts[0], 10, 64)
			if err != nil {
				return errors.New("invalid last ID format")
			}
			newMs, err := strconv.ParseInt(newParts[0], 10, 64)
			if err != nil {
				return errors.New("invalid new ID format")
			}
			lastSeq, err := strconv.ParseInt(lastParts[1], 10, 64)
			if err != nil {
				return errors.New("invalid last ID format")
			}
			newSeq, err := strconv.ParseInt(newParts[1], 10, 64)
			if err != nil {
				return errors.New("invalid new ID format")
			}
			if newMs < lastMs || (newMs == lastMs && newSeq <= lastSeq) {
				return errors.New("ID must be greater than the last entry's ID")
			}
		}
	}

	if !ok || len(item.Value.Streams) == 0 {
		d.data[key] = Entry{
			Value{
				Type:    TypeStream,
				Streams: make([]Stream, 0, len(pairs)),
			},
		}
	}
	stream := Stream{
		Key:     key,
		ID:      ID,
		Entries: pairs,
	}
	item = d.data[key]
	item.Value.Streams = append(item.Value.Streams, stream)
	d.data[key] = item

	return nil
}

type XRangeResp struct {
	ID      string
	Entries [][2]string
}

func (s *Storage) XRange(key, start, end string, db int) ([]XRangeResp, error) {
	if db >= 10 {
		return nil, fmt.Errorf("invalid database %d", db)
	}

	return s.databases[db].XRange(key, start, end)
}

func (d *Database) XRange(key, start, end string) ([]XRangeResp, error) {
	d.mu.RLock()
	item, ok := d.data[key]
	d.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%s not exists", key)
	}
	if len(item.Value.Streams) == 0 {
		return nil, fmt.Errorf("%s is not stream", key)
	}
	found := make([]Stream, 0)
	startInt, _ := strconv.Atoi(start)
	endInt, _ := strconv.Atoi(end)
	for _, stream := range item.Value.Streams {
		id := strings.Split(stream.ID, "-")[0]
		idMils, _ := strconv.Atoi(id)
		if (strings.HasPrefix(start, "+") && idMils <= endInt) || (strings.HasPrefix(end, "-") && idMils >= startInt) || (idMils >= startInt && idMils <= endInt) {
			found = append(found, stream)
		}
	}
	resp := make([]XRangeResp, 0, len(found))
	for _, f := range found {
		resp = append(resp, XRangeResp{ID: f.ID, Entries: f.Entries})
	}
	return resp, nil
}

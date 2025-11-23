package storage

import (
	"testing"
	"time"
)

func TestStorage_Set_Get_Basic(t *testing.T) {
	s := NewStorage()

	if err := s.Set("hello", "world", 100*time.Second, 0); err != nil {
		t.Fatal(err)
	}

	e, err := s.Get("hello", 0)

	if err != nil {
		t.Fatal(err)
	}
	if e == nil || e.Value.String != "world" {
		t.Fatalf("got %v, want world", e)
	}
}

func TestStorage_Get_NonExistent(t *testing.T) {
	s := NewStorage()

	e, err := s.Get("missing", 0)
	if err != nil {
		t.Fatal(err)
	}
	if e != nil {
		t.Fatalf("got %v, want nil", e)
	}
}

func TestStorage_Expiry(t *testing.T) {
	s := NewStorage()

	s.Set("temp", "value", 50*time.Millisecond, 0)

	time.Sleep(100 * time.Millisecond)

	e, err := s.Get("temp", 0)
	if err != nil {
		t.Fatal(err)
	}
	if e != nil {
		t.Fatal("key should have expired")
	}
}

func TestStorage_DatabaseIsolation(t *testing.T) {
	s := NewStorage()

	s.Set("key", "db0", 100*time.Second, 0)
	s.Set("key", "db1", 100*time.Second, 1)

	e0, _ := s.Get("key", 0)
	e1, _ := s.Get("key", 1)

	if e0.Value.String != "db0" || e1.Value.String != "db1" {
		t.Fatalf("databases not isolated: %v %v", e0, e1)
	}
}

func TestStorage_InvalidDB(t *testing.T) {
	s := NewStorage()

	if err := s.Set("k", "v", 0, 999); err == nil {
		t.Fatal("expected error for invalid db")
	}
	if _, err := s.Get("k", 999); err == nil {
		t.Fatal("expected error for invalid db")
	}
}

func TestStorage_ConcurrentAccess(t *testing.T) {
	s := NewStorage()
	done := make(chan bool)

	go func() {
		for i := 0; i < 1000; i++ {
			s.Set("key", "value", 0, 0)
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 1000; i++ {
			s.Get("key", 0)
		}
		done <- true
	}()

	<-done
	<-done
}

func TestStorage_Del(t *testing.T) {
	s := NewStorage()

	s.Set("key1", "val1", 100*time.Second, 0)
	s.Set("key2", "val2", 100*time.Second, 0)
	s.Set("key3", "val3", 100*time.Second, 1)

	if s.Del("key1", 0) != 1 {
		t.Fatal("Del should return 1")
	}
	if s.Del("key1", 0) != 0 {
		t.Fatal("Del on missing key should return 0")
	}
	if s.Del("key2", 0) != 1 {
		t.Fatal("Del should return 1")
	}
	if s.Del("key3", 0) != 0 {
		t.Fatal("Del on wrong db should return 0")
	}
	if s.Del("key3", 1) != 1 {
		t.Fatal("Del should return 1 in correct db")
	}
	if s.Del("key3", 1) != 0 {
		t.Fatal("second Del should return 0")
	}

	// Check keys are really gone
	if entry, err := s.Get("key1", 0); entry != nil || err != nil {
		t.Fatal("key1 should be deleted")
	}
	if entry, err := s.Get("key2", 0); entry != nil || err != nil {
		t.Fatal("key2 should be deleted")
	}
	if entry, err := s.Get("key3", 0); entry != nil || err != nil {
		t.Fatal("key3 should be deleted")
	}
}

func TestStorage_Del_InvalidDB(t *testing.T) {
	s := NewStorage()
	s.Set("key", "value", 100*time.Second, 0)

	if s.Del("key", 999) != 0 {
		t.Fatal("Del on invalid db should return 0")
	}
	if entry, err := s.Get("key", 0); entry == nil || err != nil {
		t.Fatal("key should not be deleted from valid db")
	}
}

func TestStorage_Del_Concurrent(t *testing.T) {
	s := NewStorage()
	s.Set("key", "value", 100*time.Second, 0)

	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func() {
			s.Del("key", 0)
			s.Del("missing", 0)
			done <- true
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}
func TestStorage_Flush(t *testing.T) {
	s := NewStorage()

	s.Set("k1", "v1", 100*time.Second, 0)
	s.Set("k2", "v2", 100*time.Second, 1)
	s.Set("k3", "v3", 100*time.Second, 9)

	s.Flush()
	if entry, err := s.Get("k1", 0); entry != nil || err != nil {
		t.Fatal("k1 should be removed")
	}
	if entry, err := s.Get("k2", 1); entry != nil || err != nil {
		t.Fatal("k2 should be removed")
	}
	if entry, err := s.Get("k3", 9); entry != nil || err != nil {
		t.Fatal("k3 should be removed")
	}

}

func TestStorage_Flush_ConcurrentWithSet(t *testing.T) {
	s := NewStorage()

	for i := 0; i < 100; i++ {
		s.Set("key", "value", 0, i%10)
	}

	done := make(chan bool)
	go func() {
		s.Flush()
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			s.Set("temp", "temp", 0, 0)
		}
		done <- true
	}()

	<-done
	<-done
}

func TestRRange(t *testing.T) {
	s := NewStorage()

	s.RPush("mylist", []string{"a", "b", "c", "d", "e"}, 0)

	tests := []struct {
		key  string
		from string
		to   string
		want string
	}{
		{"mylist", "0", "5", "a,b,c,d,e"},
		{"mylist", "1", "4", "b,c,d"},
		{"mylist", "0", "1", "a"},
		{"mylist", "2", "2", ""},
		{"mylist", "0", "0", ""},
		{"missing", "0", "5", ""},
	}

	for _, tt := range tests {
		got, err := s.RRange(tt.key, tt.from, tt.to, 0)
		if err != nil {
			t.Fatalf("RRange(%q, %s, %s) error: %v", tt.key, tt.from, tt.to, err)
		}
		if got != tt.want {
			t.Errorf("RRange(%q, %s, %s) = %q, want %q", tt.key, tt.from, tt.to, got, tt.want)
		}
	}
}

func TestRRange_InvalidArgs(t *testing.T) {
	s := NewStorage()

	tests := []struct {
		key  string
		from string
		to   string
	}{
		{"k", "x", "5"},
		{"k", "1", "y"},
		{"k", "abc", "def"},
	}

	for _, tt := range tests {
		_, err := s.RRange(tt.key, tt.from, tt.to, 0)
		if err == nil {
			t.Errorf("RRange(%q, %s, %s) should fail", tt.key, tt.from, tt.to)
		}
	}
}

func TestRRange_InvalidDB(t *testing.T) {
	s := NewStorage()
	_, err := s.RRange("k", "0", "1", 99)
	if err == nil {
		t.Fatal("expected error for invalid db")
	}
}

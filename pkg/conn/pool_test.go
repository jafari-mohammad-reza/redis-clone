package conn

import (
	"net"
	"testing"
	"time"
)

func TestCreatePool(t *testing.T) {
	go func() {
		ln, err := net.Listen("tcp", ":3080")
		if err != nil {
			panic("failed to listen to 3080")
		}
		for {
			_, err := ln.Accept()
			if err != nil {
				panic("failed to accept conn")
			}
		}
	}()
	time.Sleep(time.Second)
	pool := NewConnPool(":3080", 6)
	if pool == nil {
		t.Fatal("pool is nil")
	}
	if len(pool.conns) != 6 {
		t.Fatalf("open connections must be 6 its %d now.", len(pool.conns))
	}
	for i, conn := range pool.conns {
		if conn.RemoteAddr().String() != "127.0.0.1:3080" {
			t.Fatalf("expected conn %d to listen to  127.0.0.1:3080 now got %s.", i, conn.RemoteAddr().String())
		}
	}
}

func Test_isAlive(t *testing.T) {
	go func() {
		ln, err := net.Listen("tcp", ":3081")
		if err != nil {
			panic("failed to listen to 3081")
		}
		for {
			_, err := ln.Accept()
			if err != nil {
				panic("failed to accept conn")
			}
		}
	}()
	time.Sleep(time.Second)
	pool := NewConnPool(":3081", 6)

	t.Run("nil returns false", func(t *testing.T) {
		if pool.isAlive(nil) {
			t.Fatal("nil conn should not be alive")
		}
	})

	t.Run("healthy conn returns true", func(t *testing.T) {
		conn := pool.conns[0]
		if !pool.isAlive(conn) {
			t.Fatal("healthy conn reported dead")
		}
	})

	t.Run("closed conn returns false", func(t *testing.T) {
		c, _ := net.Pipe()
		c.Close()
		if pool.isAlive(c) {
			t.Fatal("closed conn reported alive")
		}
	})
}

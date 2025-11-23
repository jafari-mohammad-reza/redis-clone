package conn

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Pool struct {
	addr  string
	size  int
	next  atomic.Uint32
	conns []net.Conn
	mu    sync.Mutex
}

func NewConnPool(addr string, size int) *Pool {
	if size < 1 {
		size = 4
	}
	p := &Pool{addr: addr, size: size, conns: make([]net.Conn, size)}
	for i := 0; i < size; i++ {
		p.conns[i] = p.dial()
	}
	go p.healthChecker()
	return p
}

func (p *Pool) dial() net.Conn {
	conn, err := net.DialTimeout("tcp", p.addr, 3*time.Second)
	if err != nil {
		return nil
	}
	return conn
}

func (p *Pool) Get() net.Conn {
	idx := p.next.Add(1) % uint32(p.size)
	conn := p.conns[idx]

	if conn != nil && p.isAlive(conn) {
		return conn
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if old := p.conns[idx]; old != nil {
		old.Close()
	}
	p.conns[idx] = p.dial()
	return p.conns[idx]
}
func (p *Pool) isAlive(c net.Conn) bool {
	if c == nil {
		return false
	}

	if err := c.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return false
	}
	_, err := c.Write(nil)
	c.SetWriteDeadline(time.Time{})
	return err == nil
}

func (p *Pool) healthChecker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()
		alive := make([]net.Conn, 0, len(p.conns))

		for _, c := range p.conns {

			if c != nil && p.isAlive(c) {
				alive = append(alive, c)
			} else {
				if c != nil {
					c.Close()
				}
			}
		}

		for len(alive) < p.size {
			alive = append(alive, p.dial())
		}

		p.conns = alive
		p.mu.Unlock()
	}
}
func (p *Pool) HealthCheckerOnce() {
	p.mu.Lock()
	alive := make([]net.Conn, 0, len(p.conns))

	for _, c := range p.conns {
		if c != nil && p.isAlive(c) {
			alive = append(alive, c)
		} else {
			if c != nil {
				c.Close()
			}
		}
	}

	for len(alive) < p.size {
		alive = append(alive, p.dial())
	}

	p.conns = alive
	p.mu.Unlock()
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		if c != nil {
			c.Close()
		}
	}
}

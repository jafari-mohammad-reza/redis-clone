package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jafari-mohammad-reza/redis-clone/pkg/resp"
)

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", ":8090")
	if err != nil {
		log.Fatalf("failed to listen on :8090: %v", err)
	}
	defer ln.Close()

	log.Println("server listening on :8090")

	go func() {
		<-ctx.Done()
		log.Println("shutting down, closing listener...")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {

			if ctx.Err() != nil {
				log.Println("server stopped")
				return
			}
			log.Printf("accept error: %v", err)
			continue
		}

		log.Printf("new connection from %s", conn.RemoteAddr())
		go handleConn(ctx, conn)
	}
}

func handleConn(parentCtx context.Context, conn net.Conn) {
	defer conn.Close()

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go func() {
		reader := bufio.NewReader(conn)
		for {
			val, err := resp.UnmarshalOne(reader)
			if err != nil {
				if errors.Is(err, net.ErrClosed) ||
					errors.Is(err, io.EOF) ||
					isConnectionReset(err) {

					continue
				}

				cancel()
				return
			}

			if val.Typ == "array" && len(val.Array) > 0 {
				cmd := val.Array[0].Str
				if cmd == "PING" {
					resp.WriteValue(conn, resp.Value{Typ: "string", Str: "PONG"})
				} else {
					resp.WriteValue(conn, resp.Value{Typ: "string", Str: "OK"})
				}
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Printf("handling connection from %s", conn.RemoteAddr())

	for {
		select {
		case <-ctx.Done():
			log.Printf("connection %s closed: %v", conn.RemoteAddr(), ctx.Err())
			return
		case <-ticker.C:
			if _, err := conn.Write([]byte("alive\n")); err != nil {
				log.Printf("write failed for %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
	}
}
func isConnectionReset(err error) bool {
	if err == nil {
		return false
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Err.Error() == "read: connection reset by peer" {
			return true
		}

		if strings.Contains(opErr.Err.Error(), "forcibly closed") {
			return true
		}
	}
	return false
}

package main

import (
	"bufio"
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/jafari-mohammad-reza/redis-clone/pkg/conn"
	"github.com/jafari-mohammad-reza/redis-clone/pkg/resp"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)

	// create a connection pool that send each request to one of connection in pool and each connection must be replaced with new one if disconnected
	connPool := conn.NewConnPool(":8090", 6) // 6 connection

	defer connPool.Close()

	// send ping request to check if connection was successful
	conn := connPool.Get()
	if conn == nil {
		log.Fatalf("failed to get conn from conn pool")
		return
	}
	pingCmd := []any{"PING"}
	data, _ := resp.Marshal(pingCmd)
	if _, err := conn.Write(data); err != nil { // send paylaod using RESP builder
		log.Fatalf("failed to get PONG response: %s", err.Error())
		return
	}
	reader := bufio.NewReader(conn)
	val, _ := resp.UnmarshalOne(reader)
	if val.Str != "PONG" {
		log.Fatal("failed to get PONG response")
		return
	}

	defer cancel()
	<-ctx.Done()
}

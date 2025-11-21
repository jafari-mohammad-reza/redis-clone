package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/jafari-mohammad-reza/redis-clone/pkg/conn"
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

	if _, err := conn.Write([]byte("PING")); err != nil { // send paylaod using RESP builder
		log.Fatalf("failed to get PONG response: %s", err.Error())
		return
	}

	defer cancel()
	<-ctx.Done()
}

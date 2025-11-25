package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jafari-mohammad-reza/redis-clone/pkg"
	"github.com/jafari-mohammad-reza/redis-clone/pkg/conn"
	"github.com/jafari-mohammad-reza/redis-clone/pkg/resp"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, syscall.SIGINT)

	// create a connection pool that send each request to one of connection in pool and each connection must be replaced with new one if disconnected
	connPool := conn.NewConnPool(":8090", 6) // 6 connection

	defer connPool.Close()

	// send ping request to check if connection was successful
	if err := pingServer(connPool); err != nil {
		log.Fatalf("failed to ping server: %s", err.Error())
		return
	}
	// start reading user commands
	scanner := bufio.NewScanner(os.Stdin)
	for {
		conn := connPool.Get()
		fmt.Print(">>>")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			os.Exit(0)
		}
		spited := strings.Split(line, " ")
		cmd, args := spited[0], spited[1:]
		switch strings.ToUpper(cmd) {
		case string(pkg.PING_CMD), string(pkg.SET_CMD), string(pkg.GET_CMD), string(pkg.DEL_CMD), string(pkg.RPUSH_CMD), string(pkg.RLEN_CMD), string(pkg.RRANGE_CMD), string(pkg.LPOP_CMD), string(pkg.RPOP_CMD):
			resp, err := SendCmd(conn, strings.ToUpper(cmd), args...)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if resp == nil {
				fmt.Println("nil response from server. wait few seconds for reconnect")
				connPool.HealthCheckerOnce()
				continue
			}
			fmt.Println(*resp)

		default:
			fmt.Println("Invalid Command")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}

	defer cancel()
	<-ctx.Done()
}
func SendCmd(conn net.Conn, command string, args ...string) (*resp.Value, error) {
	cmd := make([]any, 0, len(args)+1)
	cmd = append(cmd, command)
	for _, arg := range args {
		cmd = append(cmd, arg)
	}
	data, err := resp.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marsha given cmd: %s", err.Error())
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to get PONG response: %s", err.Error())
	}
	reader := bufio.NewReader(conn)
	val, err := resp.UnmarshalOne(reader)
	if err != nil {
		if err.Error() == "EOF" {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to unmarshal response: %s", err.Error())
	}
	return &val, nil
}
func pingServer(connPool *conn.Pool) error {
	conn := connPool.Get()
	if conn == nil {

		return fmt.Errorf("failed to get conn from conn pool")
	}
	pingCmd := []any{"PING"}
	data, _ := resp.Marshal(pingCmd)
	if _, err := conn.Write(data); err != nil { // send paylaod using RESP builder
		return fmt.Errorf("failed to get PONG response: %s", err.Error())
	}
	reader := bufio.NewReader(conn)
	val, _ := resp.UnmarshalOne(reader)
	if val.Str != "PONG" {
		return fmt.Errorf("failed to get PONG response")
	}
	return nil
}

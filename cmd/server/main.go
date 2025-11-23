package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jafari-mohammad-reza/redis-clone/internal/storage"
	"github.com/jafari-mohammad-reza/redis-clone/pkg"
	"github.com/jafari-mohammad-reza/redis-clone/pkg/resp"
)

var once sync.Once
var keyStorage *storage.Storage

func main() {
	once.Do(func() {
		keyStorage = storage.NewStorage()
	})
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
		defer cancel()

		reader := bufio.NewReader(conn)
		for {
			cmd, err := readCommand(reader)
			if err != nil {
				if isClientDisconnect(err) {
					return
				}
				log.Printf("Protocol error from %s: %v", conn.RemoteAddr(), err)
				return
			}

			response := dispatchCommand(cmd)
			if err := resp.WriteValue(conn, response); err != nil {
				return
			}
		}
	}()

	<-ctx.Done()
}

func isClientDisconnect(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		isConnectionReset(err)
}

func readCommand(r *bufio.Reader) (*Command, error) {
	val, err := resp.UnmarshalOne(r)
	if err != nil {
		return nil, err
	}
	if val.Typ != "array" || len(val.Array) == 0 {
		return nil, fmt.Errorf("expected array, got %s", val.Typ)
	}

	cmdName := strings.ToUpper(getString(val.Array[0]))
	args := make([]string, len(val.Array)-1)
	for i, v := range val.Array[1:] {
		args[i] = getString(v)
	}

	return &Command{Name: cmdName, Args: args}, nil
}

type Command struct {
	Name string
	Args []string
}

func getString(v resp.Value) string {
	if v.Typ == "bulk" {
		return v.Bulk
	}
	return v.Str
}

func dispatchCommand(cmd *Command) resp.Value {
	switch cmd.Name {
	case string(pkg.PING_CMD):
		return handlePing(cmd)
	case string(pkg.SET_CMD):
		return handleSet(cmd)
	case string(pkg.GET_CMD):
		return handleGet(cmd)
	case string(pkg.DEL_CMD):
		return handleDel(cmd)
	case string(pkg.RPUSH_CMD):
		return handleRPush(cmd)
	case string(pkg.RLEN_CMD):
		return handleRLen(cmd)
	default:
		return resp.Value{Typ: "error", Str: "ERR unknown command '" + cmd.Name + "'"}
	}
}

func handlePing(cmd *Command) resp.Value {
	if len(cmd.Args) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}
	return resp.Value{Typ: "bulk", Bulk: cmd.Args[0]}
}
func handleRPush(cmd *Command) resp.Value {
	if len(cmd.Args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'RPUSH' command"}
	}

	key := cmd.Args[0]
	items := cmd.Args[1:]

	length, err := keyStorage.RPush(key, items, 0)
	if err != nil {
		return resp.Value{Typ: "error", Str: "ERR " + err.Error()}
	}

	return resp.Value{Typ: "string", Str: strconv.Itoa(length)}
}
func handleRLen(cmd *Command) resp.Value {
	if len(cmd.Args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'GET' command"}
	}

	length, err := keyStorage.RLen(cmd.Args[0], 0)
	if err != nil {
		return resp.Value{Typ: "null"}
	}
	fmt.Printf("length: %v\n", length)
	return resp.Value{Typ: "string", Str: strconv.Itoa(length)}
}
func handleSet(cmd *Command) resp.Value {
	if len(cmd.Args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'SET' command"}
	}

	key := cmd.Args[0]
	value := cmd.Args[1]
	expiry := time.Duration(0)

	if len(cmd.Args) >= 3 {
		if seconds, err := strconv.Atoi(cmd.Args[2]); err == nil {
			expiry = time.Duration(seconds) * time.Second
		}
	}

	if err := keyStorage.Set(key, value, expiry, 0); err != nil {
		return resp.Value{Typ: "error", Str: "ERR " + err.Error()}
	}

	return resp.Value{Typ: "string", Str: "OK"}
}

func handleGet(cmd *Command) resp.Value {
	if len(cmd.Args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'GET' command"}
	}

	entry, err := keyStorage.Get(cmd.Args[0], 0)
	if err != nil {
		return resp.Value{Typ: "error", Str: err.Error()}
	}
	if entry == nil {
		return resp.Value{Typ: "null"}
	}
	return resp.Value{Typ: "bulk", Bulk: entry.Value.String}
}

func handleDel(cmd *Command) resp.Value {
	if len(cmd.Args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'GET' command"}
	}

	count := strconv.Itoa(keyStorage.Del(cmd.Args[0], 0))

	return resp.Value{Typ: "bulk", Str: count}
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

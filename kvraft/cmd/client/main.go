// Command client is a small failover test client for the cluster. It keeps
// writing keys and, when a node stops answering, moves to the next one.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	addrsFlag := flag.String("addrs", "localhost:6380,localhost:6381,localhost:6382", "comma separated RESP addresses")
	interval := flag.Duration("interval", 10*time.Millisecond, "delay between writes")
	flag.Parse()

	addrs := splitAndTrim(*addrsFlag)

	active := 0
	var conn net.Conn
	var reader *bufio.Reader

	dial := func() bool {
		for i := 0; i < len(addrs); i++ {
			c, err := net.DialTimeout("tcp", addrs[active], time.Second)
			if err == nil {
				conn, reader = c, bufio.NewReader(c)
				return true
			}
			active = (active + 1) % len(addrs)
		}
		return false
	}

	for !dial() {
		log.Printf("no node reachable, retrying")
		time.Sleep(time.Second)
	}
	defer conn.Close()
	log.Printf("connected to %s", addrs[active])

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	var (
		writes      int
		lastSuccess time.Time
		maxGap      time.Duration
		hadFailure  bool
	)

	go func() {
		<-stop
		log.Printf("stopped after %d writes; longest gap with no writes: %v", writes, maxGap)
		os.Exit(0)
	}()

	for {
		writes++
		key := "k" + strconv.Itoa(writes%1000)
		value := strconv.Itoa(writes)

		if err := set(conn, reader, key, value); err != nil {
			hadFailure = true
			conn.Close()
			active = (active + 1) % len(addrs)
			for !dial() {
				time.Sleep(200 * time.Millisecond)
			}
			log.Printf("switched to %s after error: %v", addrs[active], err)
			time.Sleep(*interval)
			continue
		}

		if hadFailure {
			if gap := time.Since(lastSuccess); !lastSuccess.IsZero() && gap > maxGap {
				maxGap = gap
			}
			hadFailure = false
		}
		lastSuccess = time.Now()

		if writes%1000 == 0 {
			log.Printf("%d writes ok (node %s, max gap %v)", writes, addrs[active], maxGap)
		}
		time.Sleep(*interval)
	}
}

// set sends one SET command and waits for the +OK reply.
func set(conn net.Conn, reader *bufio.Reader, key, value string) error {
	cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return err
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "+") {
		return fmt.Errorf("server replied %q", strings.TrimSpace(line))
	}
	return nil
}

// splitAndTrim turns "a, b ,c" into ["a", "b", "c"].
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

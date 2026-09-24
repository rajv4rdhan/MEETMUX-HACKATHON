package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {
	addrsFlag := flag.String("addrs", "localhost:6380,localhost:6381,localhost:6382", "comma separated RESP addresses")
	interval := flag.Duration("interval", 10*time.Millisecond, "delay between writes")
	flag.Parse()

	addrs := strings.Split(*addrsFlag, ",")

	active := 0
	conn, reader, active := dial(addrs, active)
	for conn == nil {
		log.Printf("no node reachable, retrying")
		time.Sleep(time.Second)
		conn, reader, active = dial(addrs, active)
	}
	defer conn.Close()
	log.Printf("connected to %s", addrs[active])

	var (
		writes      int
		lastSuccess time.Time
		maxGap      time.Duration
		hadFailure  bool
	)

	for {
		writes++
		key := "k" + strconv.Itoa(writes%1000)
		value := strconv.Itoa(writes)

		if err := set(conn, reader, key, value); err != nil {
			hadFailure = true
			conn.Close()
			active = (active + 1) % len(addrs)
			for {
				conn, reader, active = dial(addrs, active)
				if conn != nil {
					break
				}
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

func dial(addrs []string, active int) (net.Conn, *bufio.Reader, int) {
	for i := 0; i < len(addrs); i++ {
		conn, err := net.DialTimeout("tcp", addrs[active], time.Second)
		if err == nil {
			return conn, bufio.NewReader(conn), active
		}
		active = (active + 1) % len(addrs)
	}
	return nil, nil, active
}

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

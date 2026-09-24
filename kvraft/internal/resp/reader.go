package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// ErrProtocol marks a command that does not follow the RESP array format.
var ErrProtocol = errors.New("protocol error")

// ReadCommand reads one RESP array of bulk strings, for example
// *2\r\n$3\r\nGET\r\n$1\r\nk\r\n, and returns the arguments.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, ErrProtocol
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 {
		return nil, ErrProtocol
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		arg, err := readBulk(r)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

// readBulk reads one $len\r\ndata\r\n bulk string.
func readBulk(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", ErrProtocol
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 {
		return "", ErrProtocol
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// readLine reads a CRLF terminated line and returns it without the CRLF.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

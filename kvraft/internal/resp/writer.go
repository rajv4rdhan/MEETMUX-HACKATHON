package resp

import "strconv"

// Reply is an encoded RESP response ready to send to a client.
type Reply []byte

// OK returns the simple string reply +OK.
func OK() Reply {
	return Reply("+OK\r\n")
}

// Error returns a simple error reply.
func Error(msg string) Reply {
	return Reply("-ERR " + msg + "\r\n")
}

// Nil returns a null bulk string.
func Nil() Reply {
	return Reply("$-1\r\n")
}

// Bulk returns a bulk string reply.
func Bulk(value string) Reply {
	return Reply("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
}

// Int returns an integer reply.
func Int(n int) Reply {
	return Reply(":" + strconv.Itoa(n) + "\r\n")
}

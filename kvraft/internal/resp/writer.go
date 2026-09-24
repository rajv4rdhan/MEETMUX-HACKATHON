package resp

import "strconv"

type Reply []byte

func OK() Reply {
	return Reply("+OK\r\n")
}

func Error(msg string) Reply {
	return Reply("-ERR " + msg + "\r\n")
}

func Nil() Reply {
	return Reply("$-1\r\n")
}

func Bulk(value string) Reply {
	return Reply("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
}

func Int(n int) Reply {
	return Reply(":" + strconv.Itoa(n) + "\r\n")
}

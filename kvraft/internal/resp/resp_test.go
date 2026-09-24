package resp

import (
	"bufio"
	"strings"
	"testing"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name:  "two arguments",
			input: "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
			want:  []string{"GET", "k"},
		},
		{
			name:  "empty value",
			input: "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n",
			want:  []string{"SET", "k", ""},
		},
		{
			name:  "value with spaces",
			input: "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\na b c\r\n",
			want:  []string{"SET", "k", "a b c"},
		},
		{
			name:    "not an array",
			input:   "PING\r\n",
			wantErr: true,
		},
		{
			name:    "bad array length",
			input:   "*x\r\n",
			wantErr: true,
		},
		{
			name:    "truncated bulk",
			input:   "*2\r\n$3\r\nGET\r\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadCommand(bufio.NewReader(strings.NewReader(tt.input)))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ReadCommand(%q) = %v, nil; want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadCommand(%q) unexpected error: %v", tt.input, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ReadCommand(%q) = %v; want %v", tt.input, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("ReadCommand(%q) = %v; want %v", tt.input, got, tt.want)
				}
			}
		})
	}
}

func TestReplyEncodings(t *testing.T) {
	tests := []struct {
		name string
		got  Reply
		want string
	}{
		{"ok", OK(), "+OK\r\n"},
		{"nil", Nil(), "$-1\r\n"},
		{"bulk", Bulk("hi"), "$2\r\nhi\r\n"},
		{"int", Int(7), ":7\r\n"},
		{"error", Error("nope"), "-ERR nope\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.got) != tt.want {
				t.Fatalf("got %q; want %q", tt.got, tt.want)
			}
		})
	}
}

package store

import "testing"

func TestStoreOps(t *testing.T) {
	tests := []struct {
		name   string
		ops    func(*Store)
		key    string
		want   string
		wantOK bool
	}{
		{
			name:   "missing key",
			ops:    func(*Store) {},
			key:    "nope",
			want:   "",
			wantOK: false,
		},
		{
			name:   "set then get",
			ops:    func(s *Store) { s.Set("k", "v") },
			key:    "k",
			want:   "v",
			wantOK: true,
		},
		{
			name:   "overwrite",
			ops:    func(s *Store) { s.Set("k", "v"); s.Set("k", "w") },
			key:    "k",
			want:   "w",
			wantOK: true,
		},
		{
			name:   "delete removes key",
			ops:    func(s *Store) { s.Set("k", "v"); s.Del("k") },
			key:    "k",
			want:   "",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			tt.ops(s)
			got, ok := s.Get(tt.key)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("Get(%q) = %q, %v; want %q, %v", tt.key, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestDelReportsExistence(t *testing.T) {
	s := New()
	s.Set("k", "v")
	if !s.Del("k") {
		t.Fatal("Del existing key = false; want true")
	}
	if s.Del("k") {
		t.Fatal("Del missing key = true; want false")
	}
}

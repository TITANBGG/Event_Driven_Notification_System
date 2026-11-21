package api_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/api"
	"github.com/TITANBGG/notification-system/internal/repository"
)

func FuzzDecodeCursor(f *testing.F) {
	validCursor, err := api.EncodeCursor(repository.Cursor{
		CreatedAt: time.Date(2026, 11, 21, 10, 0, 0, 123456000, time.UTC),
		ID:        uuid.Must(uuid.NewV7()),
	})
	if err != nil {
		f.Fatalf("seed EncodeCursor failed: %v", err)
	}

	seeds := []string{
		"",
		"   ",
		validCursor,
		"not-base64!!!",
		"e30", // {}
		"eyJ0IjoiMjAyNi0xMS0yMVQxMDowMDowMFoiLCJpZCI6bnVsbH0",                                              // nil/null fields
		"eyJ0IjoiMDAwMS0wMS0wMVQwMDowMDowMFoiLCJpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCJ9", // zero time + nil uuid
		"eyJleHRyYSI6MX0",   // unknown field
		validCursor + "e30", // trailing bytes
		string([]byte{0xff, 0x00, 0xfe, 'a', '='}),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		c, err := api.DecodeCursor(raw)
		if err != nil {
			if c != nil {
				t.Fatalf("DecodeCursor returned both cursor and error: %+v, %v", c, err)
			}
			return
		}
		if c == nil {
			return
		}
		if c.CreatedAt.IsZero() || c.ID == uuid.Nil {
			t.Fatalf("DecodeCursor returned zero time or nil UUID without error: %+v", c)
		}

		reencoded, encErr := api.EncodeCursor(*c)
		if encErr != nil {
			t.Fatalf("EncodeCursor failed on decoded cursor: %v", encErr)
		}
		c2, err2 := api.DecodeCursor(reencoded)
		if err2 != nil || c2 == nil {
			t.Fatalf("round-trip DecodeCursor failed: err=%v cursor=%+v", err2, c2)
		}
		if !c.CreatedAt.Equal(c2.CreatedAt) || c.ID != c2.ID {
			t.Fatalf("round-trip mismatch: %+v vs %+v", c, c2)
		}
	})
}

package protocol

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewEventIDIs26CharactersOfCrockfordBase32(t *testing.T) {
	id, err := NewEventID()
	if err != nil {
		t.Fatalf("generate id: %v", err)
	}
	if len(id) != eventIDLength {
		t.Fatalf("expected %d characters, got %d", eventIDLength, len(id))
	}
	for _, r := range id {
		if !strings.ContainsRune(crockford, r) {
			t.Fatalf("character %q is not in the Crockford alphabet", r)
		}
	}
}

func TestNewEventIDEncodesTheTimestamp(t *testing.T) {
	at := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC).Add(123 * time.Millisecond)

	id, err := NewEventIDAt(at)
	if err != nil {
		t.Fatalf("generate id: %v", err)
	}

	decoded, err := decodeULIDForTest(t, id)
	if err != nil {
		t.Fatalf("decode id: %v", err)
	}
	millis := int64(decoded[0])<<40 | int64(decoded[1])<<32 | int64(decoded[2])<<24 |
		int64(decoded[3])<<16 | int64(decoded[4])<<8 | int64(decoded[5])
	if got := time.UnixMilli(millis).UTC(); !got.Equal(at) {
		t.Fatalf("expected the timestamp %s, got %s", at, got)
	}
}

func TestNewEventIDIsStrictlyIncreasingWithinAMillisecond(t *testing.T) {
	at := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)

	previous := ""
	for i := 0; i < 1000; i++ {
		id, err := NewEventIDAt(at)
		if err != nil {
			t.Fatalf("generate id: %v", err)
		}
		if previous != "" && id <= previous {
			t.Fatalf("identifier %q is not greater than %q", id, previous)
		}
		previous = id
	}
}

func TestNewEventIDIsSafeForConcurrentUse(t *testing.T) {
	const goroutines = 8
	const perGoroutine = 200

	var (
		mu   sync.Mutex
		seen = map[string]bool{}
		wg   sync.WaitGroup
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				id, err := NewEventID()
				if err != nil {
					t.Errorf("generate id: %v", err)
					return
				}
				mu.Lock()
				if seen[id] {
					t.Errorf("duplicate identifier %q", id)
				}
				seen[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("expected %d unique identifiers, got %d", goroutines*perGoroutine, len(seen))
	}
}

func TestParseEventTime(t *testing.T) {
	parsed, err := ParseEventTime("2026-11-13T10:00:00.123Z")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.UTC().Format(time.RFC3339Nano) != "2026-11-13T10:00:00.123Z" {
		t.Fatalf("unexpected value %s", parsed)
	}

	if _, err := ParseEventTime("not a timestamp"); err == nil {
		t.Fatal("expected an error")
	}
}

// decodeULIDForTest reverses the encoder so the test can check the timestamp
// without depending on an external ULID package.
func decodeULIDForTest(t *testing.T, id string) ([16]byte, error) {
	t.Helper()

	var out [16]byte
	var buffer uint32
	var bits uint
	position := 0

	for index, r := range id {
		value := strings.IndexRune(crockford, r)
		if value < 0 {
			return out, invalidCharacterError{character: r}
		}
		count := uint(5)
		if index == 0 {
			// The first character carries only the top three bits.
			value &= 0x1F
			count = 3
		}
		buffer = buffer<<count | uint32(value)
		bits += count
		for bits >= 8 {
			bits -= 8
			out[position] = byte(buffer >> bits)
			position++
			buffer &= (1 << bits) - 1
		}
	}
	return out, nil
}

type invalidCharacterError struct{ character rune }

func (e invalidCharacterError) Error() string {
	return "invalid ULID character"
}

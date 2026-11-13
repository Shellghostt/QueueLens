package protocol

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// crockford is the base32 alphabet used by ULID.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// eventIDLength is the number of characters in a ULID.
const eventIDLength = 26

var (
	eventIDMu   sync.Mutex
	eventIDLast int64
	eventIDRand [10]byte
)

// NewEventID returns a lexicographically sortable identifier.
//
// It is a ULID: 48 bits of millisecond timestamp followed by 80 bits of
// randomness, encoded in Crockford base32. Sortable identifiers matter here
// because events are ordered by time, not by insertion.
//
// The implementation is self-contained on purpose: the protocol package must
// not force a dependency on every adapter that imports it.
func NewEventID() (string, error) {
	return newEventIDAt(time.Now())
}

// NewEventIDAt returns an identifier for the given instant. Exported for
// tests, which need reproducible identifiers.
func NewEventIDAt(at time.Time) (string, error) {
	return newEventIDAt(at)
}

func newEventIDAt(at time.Time) (string, error) {
	millis := at.UTC().UnixMilli()

	eventIDMu.Lock()
	defer eventIDMu.Unlock()

	if millis == eventIDLast {
		// Same millisecond: increment the random part instead of drawing a new
		// one, so the identifiers stay strictly increasing.
		incrementRandom()
	} else {
		eventIDLast = millis
		if _, err := rand.Read(eventIDRand[:]); err != nil {
			return "", fmt.Errorf("read random bytes: %w", err)
		}
	}

	var id [16]byte
	id[0] = byte(millis >> 40)
	id[1] = byte(millis >> 32)
	id[2] = byte(millis >> 24)
	id[3] = byte(millis >> 16)
	id[4] = byte(millis >> 8)
	id[5] = byte(millis)
	copy(id[6:], eventIDRand[:])

	return encodeULID(id), nil
}

func incrementRandom() {
	for i := len(eventIDRand) - 1; i >= 0; i-- {
		eventIDRand[i]++
		if eventIDRand[i] != 0 {
			return
		}
	}
}

func encodeULID(id [16]byte) string {
	out := make([]byte, eventIDLength)

	high := uint64(id[0])<<56 | uint64(id[1])<<48 | uint64(id[2])<<40 | uint64(id[3])<<32 |
		uint64(id[4])<<24 | uint64(id[5])<<16 | uint64(id[6])<<8 | uint64(id[7])
	low := uint64(id[8])<<56 | uint64(id[9])<<48 | uint64(id[10])<<40 | uint64(id[11])<<32 |
		uint64(id[12])<<24 | uint64(id[13])<<16 | uint64(id[14])<<8 | uint64(id[15])

	// bit returns the index-th most significant bit of the 128-bit value.
	bit := func(index int) uint32 {
		if index < 64 {
			return uint32((high >> uint(63-index)) & 1)
		}
		return uint32((low >> uint(127-index)) & 1)
	}

	// 128 bits into 26 characters: the first character carries the top three
	// bits and every other character carries five.
	value := bit(0)<<2 | bit(1)<<1 | bit(2)
	out[0] = crockford[value]

	for position := 1; position < eventIDLength; position++ {
		base := 3 + (position-1)*5
		value = 0
		for i := 0; i < 5; i++ {
			value = value<<1 | bit(base+i)
		}
		out[position] = crockford[value]
	}
	return string(out)
}

// ParseEventTime reads a timestamp the way the protocol writes it: RFC 3339
// with a UTC offset. It is a convenience for adapters that receive times as
// strings from a broker.
func ParseEventTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

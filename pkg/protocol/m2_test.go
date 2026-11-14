package protocol

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestErrorTruncationKeepsUTF8AndRejectsDurationOverflow(t *testing.T) {
	e := Event{V: 1, TS: time.Now(), Queue: "q", JobID: "j", State: StateFailed, Error: strings.Repeat("界", 400)}
	e.Normalize(DefaultLimits())
	if !utf8.ValidString(e.Error) || len(e.Error) > 1024 {
		t.Fatal("bad UTF-8 truncation")
	}
	duration := 2147483648
	e.DurationMS = &duration
	if e.Validate(time.Now(), DefaultLimits()) == nil {
		t.Fatal("duration overflow accepted")
	}
}

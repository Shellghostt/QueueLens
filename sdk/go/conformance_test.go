package queuelens

import (
	"encoding/json"
	"github.com/shellghostt/queuelens/pkg/protocol"
	"os"
	"testing"
	"time"
)

func TestSharedProtocolConformance(t *testing.T) {
	data, err := os.ReadFile("../../tests/conformance/events.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Now   time.Time
		Cases []struct {
			Name  string
			Valid bool
			Event json.RawMessage
		}
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			var e protocol.Event
			err := json.Unmarshal(tc.Event, &e)
			if err == nil {
				err = e.Validate(fixture.Now, protocol.DefaultLimits())
			}
			if (err == nil) != tc.Valid {
				t.Fatalf("valid=%v error=%v", tc.Valid, err)
			}
		})
	}
}

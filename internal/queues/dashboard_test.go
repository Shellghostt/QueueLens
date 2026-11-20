package queues

import (
	"fmt"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/adapter"
)

func TestDashboardBrokerPageVisitAndMaterializationBounds(t *testing.T) {
	s := NewStore()
	counts := make([]adapter.Counts, 10000)
	for i := range counts {
		counts[i] = adapter.Counts{Queue: fmt.Sprintf("queue-%05d", 9999-i), Waiting: int64(i)}
	}
	s.Apply("test", counts, time.Now())
	page, _, err := s.Page("queue-00099", 200)
	if err != nil || len(page) != 201 || page[0].Queue != "queue-00100" || page[200].Queue != "queue-00300" {
		t.Fatalf("bounded candidates: length=%d error=%v", len(page), err)
	}
	counts = append(counts, adapter.Counts{Queue: "one-more"})
	s.Apply("test", counts, time.Now())
	if _, _, err = s.Page("", 200); err == nil {
		t.Fatal("broker traversal exceeded 10,000 views")
	}
	for _, limit := range []int{0, 201} {
		if _, _, err = s.Page("", limit); err == nil {
			t.Fatal("unbounded page size accepted")
		}
	}
}

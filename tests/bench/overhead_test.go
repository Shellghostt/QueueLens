package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shellghostt/queuelens/pkg/protocol"
	sdk "github.com/shellghostt/queuelens/sdk/go"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"
)

var sink uint64

//go:noinline
func job(seed uint64) {
	for i := 0; i < 10000; i++ {
		seed = seed*1664525 + 1013904223
	}
	sink = seed
}

func TestDeadCollectorJobOverhead(t *testing.T) {
	if os.Getenv("QUEUELENS_RUN_BENCH") != "1" {
		t.Skip("set QUEUELENS_RUN_BENCH=1 for the release gate")
	}
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close() // actually kill the collector
	c, err := sdk.New(sdk.Options{URL: url, Token: "0123456789abcdef", RequestTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	base, with := make([]int64, 1000000), make([]int64, 1000000)
	for i := 0; i < 10000; i++ {
		job(uint64(i))
		c.Emit(protocol.Event{Queue: "q", JobID: "j", State: protocol.StateStarted})
	}
	for i := range base {
		a := func() { start := time.Now(); job(uint64(i)); base[i] = time.Since(start).Nanoseconds() }
		b := func() {
			start := time.Now()
			job(uint64(i))
			c.Emit(protocol.Event{Queue: "q", JobID: "j", State: protocol.StateStarted})
			with[i] = time.Since(start).Nanoseconds()
		}
		if i%2 == 0 {
			a()
			b()
		} else {
			b()
			a()
		}
	}
	if err = c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Slice(base, func(i, j int) bool { return base[i] < base[j] })
	sort.Slice(with, func(i, j int) bool { return with[i] < with[j] })
	if base[500000] == 0 || base[990000] == 0 {
		t.Fatal("host clock resolution cannot measure these jobs; run the Linux overhead gate")
	}
	p50 := 100 * (float64(with[500000])/float64(base[500000]) - 1)
	p99 := 100 * (float64(with[990000])/float64(base[990000]) - 1)
	result := map[string]any{"jobs": 1000000, "baseline_p50_ns": base[500000], "sdk_p50_ns": with[500000], "baseline_p99_ns": base[990000], "sdk_p99_ns": with[990000], "added_p50_percent": p50, "added_p99_percent": p99, "stats": c.Stats()}
	data, _ := json.Marshal(result)
	fmt.Println(string(data))
	if p50 >= 2 || p99 >= 5 {
		t.Fatalf("overhead exceeds budget: p50 %.3f%% p99 %.3f%%", p50, p99)
	}
	if c.Stats().Emitted != 1010000 || c.Stats().Dropped != 1010000 {
		t.Fatal(c.Stats())
	}
}

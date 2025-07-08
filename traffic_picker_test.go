package consul

import (
	"context"
	"testing"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
)

type fakeSubConn struct {
	id string
}

func (f *fakeSubConn) UpdateAddresses([]resolver.Address) {}
func (f *fakeSubConn) Connect()                           {}
func (f *fakeSubConn) GetOrBuildProducer(balancer.ProducerBuilder) (p balancer.Producer, close func()) {
	panic("should not call this method")
}

func TestTrafficPicker_TagPriority(t *testing.T) {
	connA := &fakeSubConn{"A"} // tags: [high]
	connB := &fakeSubConn{"B"} // tags: [low]
	connC := &fakeSubConn{"C"} // tags: [low,high]

	picker := &trafficPicker{
		subConns: []subConnWithTags{
			{conn: connA, tags: []string{"high"}},
			{conn: connB, tags: []string{"low"}},
			{conn: connC, tags: []string{"low", "high"}},
		},
	}

	ctx := metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("x-traffic-tag", "high,low"))

	// high = 1<<1 = 2, low = 1<<0 = 1
	// connC gets score = 3, connA = 2, connB = 1

	result, err := picker.Pick(balancer.PickInfo{Ctx: ctx})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	id := result.SubConn.(*fakeSubConn).id
	if id != "C" {
		t.Errorf("expected SubConn C to be picked, got %s", id)
	}
}

func TestTrafficPicker_NoMetadata_RoundRobin(t *testing.T) {
	connA := &fakeSubConn{"A"}
	connB := &fakeSubConn{"B"}

	picker := &trafficPicker{
		subConns: []subConnWithTags{
			{conn: connA},
			{conn: connB},
		},
	}

	// No traffic-tag → fallback to round robin
	ctx := context.Background()
	seen := make(map[string]bool)
	for range 10 {
		res, err := picker.Pick(balancer.PickInfo{Ctx: ctx})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen[res.SubConn.(*fakeSubConn).id] = true
	}

	if len(seen) < 2 {
		t.Errorf("round robin expected to see both conns, got: %+v", seen)
	}
}

func TestTrafficPicker_OnlyOneTagMatch(t *testing.T) {
	connA := &fakeSubConn{"A"} // tags: [x]
	connB := &fakeSubConn{"B"} // tags: [y]

	picker := &trafficPicker{
		subConns: []subConnWithTags{
			{conn: connA, tags: []string{"x"}},
			{conn: connB, tags: []string{"y"}},
		},
	}

	ctx := metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("x-traffic-tag", "x"))

	result, err := picker.Pick(balancer.PickInfo{Ctx: ctx})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SubConn.(*fakeSubConn).id != "A" {
		t.Errorf("expected A, got %s", result.SubConn.(*fakeSubConn).id)
	}
}

func TestTrafficTagsFromContext(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("x-traffic-tag", "a,b,c"))
	m := trafficTagsFromContext(ctx)
	if m["a"] != 1<<2 || m["b"] != 1<<1 || m["c"] != 1<<0 {
		t.Errorf("unexpected priorities: %+v", m)
	}
}

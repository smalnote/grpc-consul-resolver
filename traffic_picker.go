package consul

import (
	"context"
	"math/rand"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"
)

const Name = "traffic_tag"

func init() {
	balancer.Register(base.NewBalancerBuilder(Name, &trafficPickerBuilder{}, base.Config{HealthCheck: true}))
}

type subConnWithTags struct {
	conn balancer.SubConn
	tags []string
}

type trafficPickerBuilder struct{}

func (*trafficPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var subConns []subConnWithTags
	for sc, scInfo := range info.ReadySCs {
		rawTags, _ := scInfo.Address.BalancerAttributes.Value("tags").([]string)
		subConns = append(subConns, subConnWithTags{
			conn: sc,
			tags: rawTags,
		})
	}
	return &trafficPicker{
		subConns: subConns,
		next:     rand.Int31(), // 初始化一个随机轮询起点
	}
}

type trafficPicker struct {
	subConns []subConnWithTags
	next     int32 // for round robin
}

func (p *trafficPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	tagPriority := trafficTagsFromContext(info.Ctx)
	if len(tagPriority) == 0 {
		// fallback to round robin
		return p.pickRoundRobin()
	}

	// 取优先级最高的
	maxPriority := 0
	var pickedConn balancer.SubConn
	for _, sc := range p.subConns {
		currPriority := 0
		for _, tag := range sc.tags {
			currPriority += int(tagPriority[tag])
		}
		if maxPriority < currPriority {
			maxPriority = currPriority
			pickedConn = sc.conn
		}
	}
	return balancer.PickResult{SubConn: pickedConn}, nil
}

func (p *trafficPicker) pickRoundRobin() (balancer.PickResult, error) {
	if len(p.subConns) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	i := atomic.AddInt32(&p.next, 1)
	idx := int(i) % len(p.subConns)
	return balancer.PickResult{SubConn: p.subConns[idx].conn}, nil
}

func trafficTagsFromContext(ctx context.Context) map[string]uint16 {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil
	}
	tags, exists := md["x-traffic-tag"]
	if !exists || len(tags) == 0 {
		return nil
	}

	var tagList []string
	for _, entry := range tags {
		for tag := range strings.SplitSeq(entry, ",") {
			if trimmed := strings.TrimSpace(tag); trimmed != "" {
				tagList = append(tagList, trimmed)
			}
		}
	}

	// at most 16 tags
	tagList = tagList[:min(16, len(tagList))]

	// 越靠前优先级越大
	tagPriorities := make(map[string]uint16, len(tagList))
	for i, tag := range tagList {
		tagPriorities[tag] = 1 << (len(tagList) - i - 1)
	}
	return tagPriorities
}

package consul

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
)

func TestPopulateEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		input    []resolver.Address
		wantCall []resolver.Address
	}{
		{
			"one",
			[]resolver.Address{{Addr: "127.0.0.1:50051"}},
			[]resolver.Address{
				{Addr: "127.0.0.1:50051"},
			},
		},
		{
			"sorted",
			[]resolver.Address{{Addr: "227.0.0.1:50051"}, {Addr: "127.0.0.1:50051"}},
			[]resolver.Address{
				{Addr: "127.0.0.1:50051"},
				{Addr: "227.0.0.1:50051"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan []resolver.Address, len(tt.input))

			fcc := &ClientConnMock{
				UpdateStateFunc: func(state resolver.State) error {
					require.Equal(t, tt.wantCall, state.Addresses)
					return nil
				},
			}

			ctx := t.Context()
			go populateEndpoints(ctx, fcc, in)
			in <- tt.input
			time.Sleep(time.Millisecond)

			require.Equal(t, 1, len(fcc.UpdateStateCalls()))
		})
	}
}

func TestWatchConsulService(t *testing.T) {
	tests := []struct {
		name             string
		tgt              target
		services         []*api.ServiceEntry
		errorFromService error
		want             []resolver.Address
	}{
		{
			"simple",
			target{Service: "svc", Wait: time.Second},
			[]*api.ServiceEntry{
				{
					Service: &api.AgentService{Address: "127.0.0.1", Port: 1024},
				},
			},
			nil,
			[]resolver.Address{
				{
					Addr: "127.0.0.1:1024",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			out := make(chan []resolver.Address)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case got := <-out:
						for i, wantAddr := range tt.want {
							require.Equal(t, wantAddr.Addr, got[i].Addr)
						}
					}
				}
			}()
			fconsul := &servicerMock{
				ServiceFunc: func(s1, s2 string, b bool, queryOptions *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
					require.Equal(t, tt.tgt.Service, s1)
					require.Equal(t, tt.tgt.Tag, s2)
					require.Equal(t, tt.tgt.Healthy, b)
					require.Equal(t, tt.tgt.Near, queryOptions.Near)
					require.Equal(t, tt.tgt.Wait, queryOptions.WaitTime)
					require.Equal(t, tt.tgt.Dc, queryOptions.Datacenter)
					require.Equal(t, tt.tgt.AllowStale, queryOptions.AllowStale)
					require.Equal(t, tt.tgt.RequireConsistent, queryOptions.RequireConsistent)

					return tt.services, &api.QueryMeta{LastIndex: 1}, tt.errorFromService
				},
			}

			go watchConsulService(ctx, fconsul, tt.tgt, out)
			time.Sleep(5 * time.Millisecond)
		})
	}
}

package adapter

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

type mockECSClient struct {
	DescribeServicesFunc func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error)
	UpdateServiceFunc    func(ctx context.Context, input *ecs.UpdateServiceInput, opts ...func(*ecs.Options)) (*ecs.UpdateServiceOutput, error)
	DescribeClustersFunc func(ctx context.Context, input *ecs.DescribeClustersInput, opts ...func(*ecs.Options)) (*ecs.DescribeClustersOutput, error)
}

func (m *mockECSClient) DescribeServices(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
	return m.DescribeServicesFunc(ctx, input, opts...)
}

func (m *mockECSClient) UpdateService(ctx context.Context, input *ecs.UpdateServiceInput, opts ...func(*ecs.Options)) (*ecs.UpdateServiceOutput, error) {
	return m.UpdateServiceFunc(ctx, input, opts...)
}

func (m *mockECSClient) DescribeClusters(ctx context.Context, input *ecs.DescribeClustersInput, opts ...func(*ecs.Options)) (*ecs.DescribeClustersOutput, error) {
	return m.DescribeClustersFunc(ctx, input, opts...)
}

func TestECSAdapter_GetCurrentCount(t *testing.T) {
	tests := []struct {
		name      string
		mockSetup func(*mockECSClient)
		service   string
		want      int
		wantErr   bool
	}{
		{
			name: "returns running count",
			mockSetup: func(m *mockECSClient) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					return &ecs.DescribeServicesOutput{
						Services: []types.Service{
							{RunningCount: 5, DesiredCount: 5},
						},
					}, nil
				}
			},
			service: "my-service",
			want:    5,
		},
		{
			name: "service not found",
			mockSetup: func(m *mockECSClient) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					return &ecs.DescribeServicesOutput{Services: []types.Service{}}, nil
				}
			},
			service: "nonexistent",
			wantErr: true,
		},
		{
			name: "api error",
			mockSetup: func(m *mockECSClient) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					return nil, errors.New("access denied")
				}
			},
			service: "my-service",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockECSClient{}
			tt.mockSetup(mock)

			adapter := NewECSAdapterWithClient(mock, "test-cluster")
			got, err := adapter.GetCurrentCount(context.Background(), tt.service)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestECSAdapter_GetCurrentCounts(t *testing.T) {
	tests := []struct {
		name      string
		mockSetup func(*mockECSClient, *testing.T)
		services  []string
		want      map[string]int
		wantErr   bool
	}{
		{
			name: "empty services list",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					t.Fatal("should not be called")
					return nil, nil
				}
			},
			services: []string{},
			want:     map[string]int{},
		},
		{
			name: "single service",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					if !reflect.DeepEqual(input.Services, []string{"svc-a"}) {
						t.Errorf("expected services [svc-a], got %v", input.Services)
					}
					return &ecs.DescribeServicesOutput{
						Services: []types.Service{
							{ServiceName: aws.String("svc-a"), RunningCount: 3},
						},
					}, nil
				}
			},
			services: []string{"svc-a"},
			want:     map[string]int{"svc-a": 3},
		},
		{
			name: "multiple services under batch limit",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					if len(input.Services) != 3 {
						t.Errorf("expected 3 services, got %d", len(input.Services))
					}
					return &ecs.DescribeServicesOutput{
						Services: []types.Service{
							{ServiceName: aws.String("svc-a"), RunningCount: 1},
							{ServiceName: aws.String("svc-b"), RunningCount: 2},
							{ServiceName: aws.String("svc-c"), RunningCount: 3},
						},
					}, nil
				}
			},
			services: []string{"svc-a", "svc-b", "svc-c"},
			want:     map[string]int{"svc-a": 1, "svc-b": 2, "svc-c": 3},
		},
		{
			name: "batches calls when over limit",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				callCount := 0
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					callCount++
					var services []types.Service
					for _, name := range input.Services {
						services = append(services, types.Service{
							ServiceName:  aws.String(name),
							RunningCount: int32(callCount),
						})
					}
					return &ecs.DescribeServicesOutput{Services: services}, nil
				}
			},
			services: []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12"},
			want: map[string]int{
				"s1": 1, "s2": 1, "s3": 1, "s4": 1, "s5": 1,
				"s6": 1, "s7": 1, "s8": 1, "s9": 1, "s10": 1,
				"s11": 2, "s12": 2,
			},
		},
		{
			name: "service not found",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					return &ecs.DescribeServicesOutput{
						Services: []types.Service{
							{ServiceName: aws.String("svc-a"), RunningCount: 1},
						},
					}, nil
				}
			},
			services: []string{"svc-a", "svc-b"},
			wantErr:  true,
		},
		{
			name: "api error",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.DescribeServicesFunc = func(ctx context.Context, input *ecs.DescribeServicesInput, opts ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
					return nil, errors.New("access denied")
				}
			},
			services: []string{"svc-a"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockECSClient{}
			tt.mockSetup(mock, t)

			adapter := NewECSAdapterWithClient(mock, "test-cluster")
			got, err := adapter.GetCurrentCounts(context.Background(), tt.services)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestECSAdapter_Scale(t *testing.T) {
	tests := []struct {
		name      string
		mockSetup func(*mockECSClient, *testing.T)
		service   string
		desired   int
		wantErr   bool
	}{
		{
			name: "successful scale",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.UpdateServiceFunc = func(ctx context.Context, input *ecs.UpdateServiceInput, opts ...func(*ecs.Options)) (*ecs.UpdateServiceOutput, error) {
					if *input.DesiredCount != int32(10) {
						t.Errorf("expected desired count 10, got %d", *input.DesiredCount)
					}
					return &ecs.UpdateServiceOutput{}, nil
				}
			},
			service: "my-service",
			desired: 10,
		},
		{
			name: "api error",
			mockSetup: func(m *mockECSClient, t *testing.T) {
				m.UpdateServiceFunc = func(ctx context.Context, input *ecs.UpdateServiceInput, opts ...func(*ecs.Options)) (*ecs.UpdateServiceOutput, error) {
					return nil, errors.New("service not found")
				}
			},
			service: "my-service",
			desired: 10,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockECSClient{}
			tt.mockSetup(mock, t)

			adapter := NewECSAdapterWithClient(mock, "test-cluster")
			err := adapter.Scale(context.Background(), tt.service, tt.desired)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestECSAdapter_HealthCheck(t *testing.T) {
	tests := []struct {
		name      string
		mockSetup func(*mockECSClient)
		wantErr   bool
	}{
		{
			name: "healthy cluster",
			mockSetup: func(m *mockECSClient) {
				m.DescribeClustersFunc = func(ctx context.Context, input *ecs.DescribeClustersInput, opts ...func(*ecs.Options)) (*ecs.DescribeClustersOutput, error) {
					return &ecs.DescribeClustersOutput{
						Clusters: []types.Cluster{
							{Status: aws.String("ACTIVE")},
						},
					}, nil
				}
			},
		},
		{
			name: "cluster not found",
			mockSetup: func(m *mockECSClient) {
				m.DescribeClustersFunc = func(ctx context.Context, input *ecs.DescribeClustersInput, opts ...func(*ecs.Options)) (*ecs.DescribeClustersOutput, error) {
					return &ecs.DescribeClustersOutput{Clusters: []types.Cluster{}}, nil
				}
			},
			wantErr: true,
		},
		{
			name: "cluster inactive",
			mockSetup: func(m *mockECSClient) {
				m.DescribeClustersFunc = func(ctx context.Context, input *ecs.DescribeClustersInput, opts ...func(*ecs.Options)) (*ecs.DescribeClustersOutput, error) {
					return &ecs.DescribeClustersOutput{
						Clusters: []types.Cluster{
							{Status: aws.String("INACTIVE")},
						},
					}, nil
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockECSClient{}
			tt.mockSetup(mock)

			adapter := NewECSAdapterWithClient(mock, "test-cluster")
			err := adapter.HealthCheck(context.Background())

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

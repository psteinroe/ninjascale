package adapter

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/psteinroe/ninjascale/internal/config"
)

// ECSClient defines the interface for ECS operations (for mocking).
type ECSClient interface {
	DescribeServices(ctx context.Context, params *ecs.DescribeServicesInput, optFns ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error)
	UpdateService(ctx context.Context, params *ecs.UpdateServiceInput, optFns ...func(*ecs.Options)) (*ecs.UpdateServiceOutput, error)
	DescribeClusters(ctx context.Context, params *ecs.DescribeClustersInput, optFns ...func(*ecs.Options)) (*ecs.DescribeClustersOutput, error)
}

// ECSAdapter implements Adapter for AWS ECS.
type ECSAdapter struct {
	client  ECSClient
	cluster string
}

// NewECSAdapter creates a new ECS adapter.
func NewECSAdapter(cfg config.ECSConfig) (*ECSAdapter, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := ecs.NewFromConfig(awsCfg)
	return &ECSAdapter{
		client:  client,
		cluster: cfg.Cluster,
	}, nil
}

// NewECSAdapterWithClient creates an ECS adapter with a custom client (for testing).
func NewECSAdapterWithClient(client ECSClient, cluster string) *ECSAdapter {
	return &ECSAdapter{
		client:  client,
		cluster: cluster,
	}
}

// maxServicesPerBatch is the maximum number of services that can be described in a single ECS API call.
const maxServicesPerBatch = 10

func (a *ECSAdapter) GetCurrentCount(ctx context.Context, service string) (int, error) {
	out, err := a.client.DescribeServices(ctx, &ecs.DescribeServicesInput{
		Cluster:  &a.cluster,
		Services: []string{service},
	})
	if err != nil {
		return 0, fmt.Errorf("describe service: %w", err)
	}
	if len(out.Services) == 0 {
		return 0, fmt.Errorf("service not found: %s", service)
	}
	return int(out.Services[0].RunningCount), nil
}

func (a *ECSAdapter) GetCurrentCounts(ctx context.Context, services []string) (map[string]int, error) {
	if len(services) == 0 {
		return make(map[string]int), nil
	}

	result := make(map[string]int, len(services))

	// Process in batches of maxServicesPerBatch
	for i := 0; i < len(services); i += maxServicesPerBatch {
		end := i + maxServicesPerBatch
		if end > len(services) {
			end = len(services)
		}
		batch := services[i:end]

		out, err := a.client.DescribeServices(ctx, &ecs.DescribeServicesInput{
			Cluster:  &a.cluster,
			Services: batch,
		})
		if err != nil {
			return nil, fmt.Errorf("describe services: %w", err)
		}

		for _, svc := range out.Services {
			if svc.ServiceName != nil {
				result[*svc.ServiceName] = int(svc.RunningCount)
			}
		}
	}

	// Check for missing services
	for _, svc := range services {
		if _, ok := result[svc]; !ok {
			return nil, fmt.Errorf("service not found: %s", svc)
		}
	}

	return result, nil
}

func (a *ECSAdapter) Scale(ctx context.Context, service string, desired int) error {
	_, err := a.client.UpdateService(ctx, &ecs.UpdateServiceInput{
		Cluster:      &a.cluster,
		Service:      &service,
		DesiredCount: aws.Int32(int32(desired)),
	})
	if err != nil {
		return fmt.Errorf("update service: %w", err)
	}
	return nil
}

func (a *ECSAdapter) HealthCheck(ctx context.Context) error {
	out, err := a.client.DescribeClusters(ctx, &ecs.DescribeClustersInput{
		Clusters: []string{a.cluster},
	})
	if err != nil {
		return fmt.Errorf("describe cluster: %w", err)
	}
	if len(out.Clusters) == 0 {
		return fmt.Errorf("cluster not found: %s", a.cluster)
	}
	if out.Clusters[0].Status != nil && *out.Clusters[0].Status != "ACTIVE" {
		return fmt.Errorf("cluster not active: %s", *out.Clusters[0].Status)
	}
	return nil
}

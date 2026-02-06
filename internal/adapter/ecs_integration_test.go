//go:build integration

package adapter

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestECSAdapter_Integration(t *testing.T) {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "localstack/localstack:3.0",
			ExposedPorts: []string{"4566/tcp"},
			Env: map[string]string{
				"SERVICES": "ecs",
			},
			WaitingFor: wait.ForHTTP("/_localstack/health").WithPort("4566").WithStatusCodeMatcher(func(status int) bool {
				return status == 200
			}),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4566")
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	client := ecs.NewFromConfig(cfg, func(o *ecs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Setup: create cluster and service
	_, err = client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String("test-cluster"),
	})
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	_, err = client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("test-task"),
		ContainerDefinitions: []types.ContainerDefinition{
			{
				Name:  aws.String("app"),
				Image: aws.String("nginx:latest"),
			},
		},
		Cpu:    aws.String("256"),
		Memory: aws.String("512"),
	})
	if err != nil {
		t.Fatalf("failed to register task definition: %v", err)
	}

	_, err = client.CreateService(ctx, &ecs.CreateServiceInput{
		Cluster:        aws.String("test-cluster"),
		ServiceName:    aws.String("test-service"),
		TaskDefinition: aws.String("test-task"),
		DesiredCount:   aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	adapter := NewECSAdapterWithClient(client, "test-cluster")

	t.Run("HealthCheck", func(t *testing.T) {
		if err := adapter.HealthCheck(ctx); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("GetCurrentCount", func(t *testing.T) {
		count, err := adapter.GetCurrentCount(ctx, "test-service")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count < 0 {
			t.Errorf("expected count >= 0, got %d", count)
		}
	})

	t.Run("Scale", func(t *testing.T) {
		if err := adapter.Scale(ctx, "test-service", 5); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out, err := client.DescribeServices(ctx, &ecs.DescribeServicesInput{
			Cluster:  aws.String("test-cluster"),
			Services: []string{"test-service"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.Services[0].DesiredCount != int32(5) {
			t.Errorf("expected desired count 5, got %d", out.Services[0].DesiredCount)
		}
	})

	t.Run("ScaleNonexistentService", func(t *testing.T) {
		if err := adapter.Scale(ctx, "nonexistent", 5); err == nil {
			t.Error("expected error, got nil")
		}
	})
}

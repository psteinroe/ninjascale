package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type fakePrometheusAPI struct {
	value     model.Value
	warnings  promv1.Warnings
	err       error
	query     string
	evaluated time.Time
}

func (f *fakePrometheusAPI) Query(_ context.Context, query string, at time.Time, _ ...promv1.Option) (model.Value, promv1.Warnings, error) {
	f.query, f.evaluated = query, at
	return f.value, f.warnings, f.err
}

func TestPrometheusQueryPreservesVectorAndScalarTimestamp(t *testing.T) {
	at := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	observed := at.Add(-time.Second)
	cases := []struct {
		name  string
		value model.Value
	}{
		{name: "vector", value: model.Vector{&model.Sample{Value: 4, Timestamp: model.TimeFromUnixNano(observed.UnixNano())}}},
		{name: "scalar", value: &model.Scalar{Value: 4, Timestamp: model.TimeFromUnixNano(observed.UnixNano())}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakePrometheusAPI{value: tc.value}
			source := &PrometheusSource{name: "test", api: api}
			sample, present, err := source.Query(context.Background(), "up", at)
			if err != nil || !present || sample.Value != 4 || !sample.ObservedAt.Equal(observed) {
				t.Fatalf("sample=%+v present=%v err=%v", sample, present, err)
			}
			if api.query != "up" || !api.evaluated.Equal(at) {
				t.Fatalf("query=%q evaluated=%v", api.query, api.evaluated)
			}
		})
	}
}

func TestPrometheusQueryMissingErrorAndAmbiguous(t *testing.T) {
	at := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		api         *fakePrometheusAPI
		wantPresent bool
		wantErr     bool
	}{
		{name: "empty vector is missing", api: &fakePrometheusAPI{value: model.Vector{}}},
		{name: "query error", api: &fakePrometheusAPI{err: errors.New("boom")}, wantErr: true},
		{name: "multiple series are ambiguous", api: &fakePrometheusAPI{value: model.Vector{&model.Sample{}, &model.Sample{}}}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &PrometheusSource{name: "test", api: tc.api}
			_, present, err := source.Query(context.Background(), "up", at)
			if present != tc.wantPresent || (err != nil) != tc.wantErr {
				t.Fatalf("present=%v err=%v", present, err)
			}
		})
	}
}

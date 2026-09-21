package paceotel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mkbeh/pacecache"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var _ Source = (*pacecache.Cache[string, int])(nil)

type metricsSourceStub struct {
	name    string
	stats   pacecache.Stats
	statsFn func() pacecache.Stats
}

func (source *metricsSourceStub) Name() string {
	if source == nil {
		return ""
	}

	return source.name
}

func (source *metricsSourceStub) Stats() pacecache.Stats {
	if source == nil {
		return pacecache.Stats{}
	}

	if source.statsFn != nil {
		return source.statsFn()
	}

	return source.stats
}

type registrationStub struct {
	embedded.Registration

	err   error
	calls atomic.Int64
}

func (registration *registrationStub) Unregister() error {
	registration.calls.Add(1)

	return registration.err
}

func TestNewIgnoresNilOption(t *testing.T) {
	metrics := New(nil)
	if metrics == nil {
		t.Fatal("New(nil) = nil")
	}

	if err := metrics.Unregister(); err != nil {
		t.Fatalf("Unregister() error = %v", err)
	}
}

func TestMetricsRegisterValidation(t *testing.T) {
	t.Run("nil metrics", func(t *testing.T) {
		var metrics *Metrics

		err := metrics.Register(&metricsSourceStub{name: "users"})
		if err == nil || err.Error() != "paceotel: metrics is nil" {
			t.Fatalf("Register() error = %v, want metrics is nil", err)
		}
	})

	t.Run("nil source", func(t *testing.T) {
		metrics, _ := newTestMetrics(t)

		err := metrics.Register(nil)
		if err == nil || err.Error() != "paceotel: source is nil" {
			t.Fatalf("Register() error = %v, want source is nil", err)
		}
	})
}

func TestMetricsRegisterDuplicateName(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	if err := metrics.Register(&metricsSourceStub{name: "users"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	err := metrics.Register(&metricsSourceStub{name: "users"})
	const want = `paceotel: metrics source "users" already registered`
	if err == nil || err.Error() != want {
		t.Fatalf("Register() error = %v, want %q", err, want)
	}
}

func TestMetricsRegisterDuplicateUnnamedSource(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	if err := metrics.Register(&metricsSourceStub{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	err := metrics.Register(&metricsSourceStub{})
	const want = `paceotel: metrics source "" already registered`
	if err == nil || err.Error() != want {
		t.Fatalf("Register() error = %v, want %q", err, want)
	}
}

func TestMetricsRegisterAfterUnregister(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	if err := metrics.Unregister(); err != nil {
		t.Fatalf("Unregister() error = %v", err)
	}

	err := metrics.Register(&metricsSourceStub{name: "users"})
	if err == nil || err.Error() != "paceotel: metrics is unregistered" {
		t.Fatalf("Register() error = %v, want metrics is unregistered", err)
	}
}

func TestMetricsUnregisterNilSafe(t *testing.T) {
	var metrics *Metrics

	if err := metrics.Unregister(); err != nil {
		t.Fatalf("Unregister() error = %v", err)
	}
}

func TestMetricsUnregisterRepeated(t *testing.T) {
	registration := &registrationStub{}
	metrics := &Metrics{
		registrations: map[string]metric.Registration{
			"users": registration,
		},
	}

	if err := metrics.Unregister(); err != nil {
		t.Fatalf("first Unregister() error = %v", err)
	}
	if err := metrics.Unregister(); err != nil {
		t.Fatalf("second Unregister() error = %v", err)
	}

	if got := registration.calls.Load(); got != 1 {
		t.Fatalf("Registration.Unregister() calls = %d, want 1", got)
	}
}

func TestMetricsUnregisterRetriesFailures(t *testing.T) {
	firstErr := errors.New("first unregister failed")
	secondErr := errors.New("second unregister failed")

	firstFailed := &registrationStub{err: firstErr}
	secondFailed := &registrationStub{err: secondErr}
	successful := &registrationStub{}

	metrics := &Metrics{
		registrations: map[string]metric.Registration{
			"first":      firstFailed,
			"second":     secondFailed,
			"successful": successful,
		},
	}

	err := metrics.Unregister()
	if !errors.Is(err, firstErr) {
		t.Fatalf("Unregister() error = %v, want wrapped first error", err)
	}
	if !errors.Is(err, secondErr) {
		t.Fatalf("Unregister() error = %v, want wrapped second error", err)
	}
	if !strings.Contains(err.Error(), `source "first"`) {
		t.Fatalf("Unregister() error = %q, want first source name", err)
	}
	if !strings.Contains(err.Error(), `source "second"`) {
		t.Fatalf("Unregister() error = %q, want second source name", err)
	}

	if got := firstFailed.calls.Load(); got != 1 {
		t.Fatalf("first failed Unregister() calls = %d, want 1", got)
	}
	if got := secondFailed.calls.Load(); got != 1 {
		t.Fatalf("second failed Unregister() calls = %d, want 1", got)
	}
	if got := successful.calls.Load(); got != 1 {
		t.Fatalf("successful Unregister() calls = %d, want 1", got)
	}

	registerErr := metrics.Register(&metricsSourceStub{name: "new"})
	if registerErr == nil || registerErr.Error() != "paceotel: metrics is unregistered" {
		t.Fatalf(
			"Register() after failed Unregister() error = %v, want metrics is unregistered",
			registerErr,
		)
	}

	firstFailed.err = nil
	secondFailed.err = nil

	if err := metrics.Unregister(); err != nil {
		t.Fatalf("retry Unregister() error = %v", err)
	}

	if got := firstFailed.calls.Load(); got != 2 {
		t.Fatalf("first failed Unregister() calls after retry = %d, want 2", got)
	}
	if got := secondFailed.calls.Load(); got != 2 {
		t.Fatalf("second failed Unregister() calls after retry = %d, want 2", got)
	}
	if got := successful.calls.Load(); got != 1 {
		t.Fatalf("successful Unregister() calls after retry = %d, want 1", got)
	}

	if err := metrics.Unregister(); err != nil {
		t.Fatalf("third Unregister() error = %v", err)
	}
	if got := firstFailed.calls.Load(); got != 2 {
		t.Fatalf("first failed Unregister() calls after third call = %d, want 2", got)
	}
	if got := secondFailed.calls.Load(); got != 2 {
		t.Fatalf("second failed Unregister() calls after third call = %d, want 2", got)
	}
	if got := successful.calls.Load(); got != 1 {
		t.Fatalf("successful Unregister() calls after third call = %d, want 1", got)
	}
}

func TestMetricsConcurrentRegister(t *testing.T) {
	metrics, reader := newTestMetrics(t)

	const count = 16

	start := make(chan struct{})
	errorsCh := make(chan error, count)

	var waitGroup sync.WaitGroup
	for index := range count {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			<-start

			errorsCh <- metrics.Register(
				&metricsSourceStub{name: fmt.Sprintf("cache-%d", index)},
			)
		}()
	}

	close(start)
	waitGroup.Wait()
	close(errorsCh)

	for err := range errorsCh {
		if err != nil {
			t.Fatalf("Register() error = %v", err)
		}
	}

	collected := collectMetricsByName(t, collectTestMetrics(t, reader))
	points := int64GaugePoints(
		t,
		requireMetric(t, collected, wantEntryCountMetricName),
	)
	if len(points) != count {
		t.Fatalf("entry count points = %d, want %d", len(points), count)
	}

	for index := range count {
		attributes := map[string]string{
			wantCacheNameAttribute: fmt.Sprintf("cache-%d", index),
		}
		if !hasMetricAttributes(points, attributes) {
			t.Fatalf("entry count point with attributes %v not found", attributes)
		}
	}
}

func TestMetricsConcurrentDuplicateRegister(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	const count = 16

	start := make(chan struct{})
	errorsCh := make(chan error, count)

	var waitGroup sync.WaitGroup
	for range count {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			<-start

			errorsCh <- metrics.Register(&metricsSourceStub{name: "users"})
		}()
	}

	close(start)
	waitGroup.Wait()
	close(errorsCh)

	var successful int
	var duplicates int

	for err := range errorsCh {
		switch {
		case err == nil:
			successful++
		case err.Error() == `paceotel: metrics source "users" already registered`:
			duplicates++
		default:
			t.Fatalf("Register() error = %v", err)
		}
	}

	if successful != 1 {
		t.Fatalf("successful registrations = %d, want 1", successful)
	}
	if duplicates != count-1 {
		t.Fatalf("duplicate registrations = %d, want %d", duplicates, count-1)
	}
}

func TestMetricsConcurrentRegisterWithUnregister(t *testing.T) {
	metrics, _ := newTestMetrics(t)

	start := make(chan struct{})
	registerDone := make(chan error, 1)
	unregisterDone := make(chan error, 1)

	go func() {
		<-start
		registerDone <- metrics.Register(&metricsSourceStub{name: "users"})
	}()

	go func() {
		<-start
		unregisterDone <- metrics.Unregister()
	}()

	close(start)

	select {
	case registerErr := <-registerDone:
		if registerErr != nil && registerErr.Error() != "paceotel: metrics is unregistered" {
			t.Fatalf("Register() error = %v", registerErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Register() did not complete")
	}

	select {
	case err := <-unregisterDone:
		if err != nil {
			t.Fatalf("Unregister() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Unregister() did not complete")
	}

	err := metrics.Register(&metricsSourceStub{name: "after"})
	if err == nil || err.Error() != "paceotel: metrics is unregistered" {
		t.Fatalf("Register() after concurrent Unregister() error = %v, want metrics is unregistered", err)
	}
}

func TestMetricsConcurrentUnregister(t *testing.T) {
	registration := &registrationStub{}
	metrics := &Metrics{
		registrations: map[string]metric.Registration{
			"users": registration,
		},
	}

	const count = 16

	start := make(chan struct{})
	errorsCh := make(chan error, count)

	var waitGroup sync.WaitGroup
	for range count {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			<-start

			errorsCh <- metrics.Unregister()
		}()
	}

	close(start)
	waitGroup.Wait()
	close(errorsCh)

	for err := range errorsCh {
		if err != nil {
			t.Fatalf("Unregister() error = %v", err)
		}
	}

	if got := registration.calls.Load(); got != 1 {
		t.Fatalf("Registration.Unregister() calls = %d, want 1", got)
	}
}

func TestMetricsCollectConcurrentWithUnregister(t *testing.T) {
	metrics, reader := newTestMetrics(t)

	started := make(chan struct{})
	release := make(chan struct{})

	var startOnce sync.Once
	source := &metricsSourceStub{
		name: "users",
		statsFn: func() pacecache.Stats {
			startOnce.Do(func() {
				close(started)
			})
			<-release

			return pacecache.Stats{EntryCount: 1}
		},
	}

	if err := metrics.Register(source); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	collectDone := make(chan error, 1)
	go func() {
		var resourceMetrics metricdata.ResourceMetrics
		collectDone <- reader.Collect(context.Background(), &resourceMetrics)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("metric callback did not start")
	}

	unregisterDone := make(chan error, 1)
	go func() {
		unregisterDone <- metrics.Unregister()
	}()

	close(release)

	select {
	case err := <-collectDone:
		if err != nil {
			t.Fatalf("Collect() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Collect() did not complete")
	}

	select {
	case err := <-unregisterDone:
		if err != nil {
			t.Fatalf("Unregister() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Unregister() did not complete")
	}

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatalf("Collect() after Unregister() error = %v", err)
	}

	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		if scopeMetrics.Scope.Name == wantScopeName && len(scopeMetrics.Metrics) != 0 {
			t.Fatalf(
				"metrics after Unregister() = %d, want 0",
				len(scopeMetrics.Metrics),
			)
		}
	}
}

func newTestMetrics(t *testing.T) (*Metrics, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)
	metrics := New(
		WithMeterProvider(meterProvider),
	)

	t.Cleanup(func() {
		if err := metrics.Unregister(); err != nil {
			t.Errorf("Unregister() error = %v", err)
		}
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	return metrics, reader
}

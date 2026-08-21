package paceotel

import (
	"errors"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/embedded"
)

type registrationStub struct {
	embedded.Registration

	err   error
	calls atomic.Int64
}

func (registration *registrationStub) Unregister() error {
	registration.calls.Add(1)

	return registration.err
}

func TestMetricsRegistrationClose(t *testing.T) {
	underlying := &registrationStub{}
	registration := &metricsRegistration{
		registration: underlying,
	}

	registration.Close()

	if got := underlying.calls.Load(); got != 1 {
		t.Fatalf("Unregister() calls = %d, want 1", got)
	}
}

func TestMetricsRegistrationCloseNilSafe(_ *testing.T) {
	var registration *metricsRegistration
	registration.Close()

	registration = &metricsRegistration{}
	registration.Close()
}

func TestMetricsRegistrationCloseReportsUnregisterError(t *testing.T) {
	sentinel := errors.New("unregister failed")
	underlying := &registrationStub{err: sentinel}
	registration := &metricsRegistration{
		registration: underlying,
	}

	previous := otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetErrorHandler(previous)
	})

	var handled atomic.Value
	otel.SetErrorHandler(
		otel.ErrorHandlerFunc(
			func(err error) {
				handled.Store(err)
			},
		),
	)

	registration.Close()

	if got := underlying.calls.Load(); got != 1 {
		t.Fatalf("Unregister() calls = %d, want 1", got)
	}

	value := handled.Load()
	if value == nil {
		t.Fatal("OpenTelemetry error handler was not called")
	}

	err, ok := value.(error)
	if !ok {
		t.Fatalf("handled value = %T, want error", value)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("handled error = %v, want wrapped sentinel", err)
	}
}

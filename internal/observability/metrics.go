package observability

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// namespaceReplistore is the Prometheus namespace prefix for all metrics.
	namespaceReplistore = "replistore"

	// subsystemBackend is the Prometheus subsystem and the "backend" label name
	// shared by the backend metrics.
	subsystemBackend = "backend"

	subsystemFSOp = "fsop"

	// labelType is the "type" label name shared by the backend metrics.
	labelType = "type"

	resultSuccess = "success"
	resultError   = "error"
)

// resultLabel maps an operation error to the coarse "result" histogram label.
// Latency histograms stay coarse on purpose: granular error categories belong on
// the cheap *_ops_total counters (see classifyError), not on histogram buckets
// where they multiply cardinality.
func resultLabel(err error) string {
	if err != nil {
		return resultError
	}

	return resultSuccess
}

// classifyError maps err to a bounded set of categories for the "error" counter
// label, so error rates can be tracked per kind without unbounded cardinality.
// The categories are derived with errors.Is, so wrapped errors are matched too.
func classifyError(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, os.ErrNotExist):
		return "not_found"
	case errors.Is(err, os.ErrPermission):
		return "permission"
	case errors.Is(err, os.ErrExist):
		return "exists"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	default:
		return resultError
	}
}

// fsOpDuration tracks the latency distribution of FUSE operations, labelled by
// operation name. The histogram's _count timeseries doubles as the per-op call
// counter, so a single metric serves both throughput and latency monitoring.
//
// Buckets target the FUSE hot path: most operations resolve from cache in tens
// of microseconds, while backend-bound ones (read, write) stretch into tens of
// milliseconds. The range spans 100µs to ~5s to capture both and any tail.
var fsOpDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: namespaceReplistore,
		Subsystem: "fsop",
		Name:      "duration_seconds",
		Help:      "Latency of FUSE filesystem operations in seconds.",
		Buckets: []float64{
			0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
			0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
		},
	},
	[]string{"op"},
)

// backendPingDuration tracks the latency distribution of backend health probes,
// labelled by backend name, type, and outcome. The "result" label keeps failed
// probes (which run to the ping timeout) from skewing the success latency, while
// still recording how long failures take. The _count timeseries gives probe rate.
//
// Buckets span 1ms to 3s: a local backend answers in well under a millisecond,
// an SMB backend in single-digit milliseconds, and the ping timeout sits at 2s.
var backendPingDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: namespaceReplistore,
		Subsystem: subsystemBackend,
		Name:      "ping_duration_seconds",
		Help:      "Latency of backend health-check pings in seconds.",
		Buckets: []float64{
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3,
		},
	},
	[]string{subsystemBackend, labelType, "result"},
)

// backendOpDuration tracks the latency distribution of individual backend
// operations, labelled by backend name, type, operation, and outcome. Unlike
// fsOpDuration (which measures the FUSE boundary, with cache hits and replica
// fan-out folded in), this isolates the cost of a single backend round-trip, so
// a slow replica is visible on its own rather than hidden in the aggregate.
//
// Buckets span 500µs to 10s: a local backend serves in well under a
// millisecond, an SMB backend in milliseconds, and a degraded one can stretch
// into seconds before timing out.
var backendOpDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: namespaceReplistore,
		Subsystem: subsystemBackend,
		Name:      "op_duration_seconds",
		Help:      "Latency of individual backend operations in seconds.",
		Buckets: []float64{
			0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	},
	[]string{subsystemBackend, labelType, "op", "result"},
)

// fsOpsTotal counts FUSE operations, labelled by operation and classified error.
// It is the canonical source for kernel-facing QPS (rate over all errors) and
// error rate (filter error!="ok"). Kept separate from the latency histogram so
// the granular error label carries no histogram-bucket cost.
var fsOpsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: namespaceReplistore,
		Subsystem: subsystemFSOp,
		Name:      "ops_total",
		Help:      "Total FUSE operations, labelled by operation and classified error.",
	},
	[]string{"op", resultError},
)

// backendOpsTotal counts individual backend operations, labelled by backend,
// type, operation, and classified error. Canonical source for per-backend QPS
// and error rate per error kind.
var backendOpsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: namespaceReplistore,
		Subsystem: subsystemBackend,
		Name:      "ops_total",
		Help:      "Total backend operations, labelled by backend, type, operation, and classified error.",
	},
	[]string{subsystemBackend, labelType, "op", resultError},
)

// fsOpBytes counts bytes moved across the FUSE boundary, labelled by operation
// (read or write). Combined with the duration histogram it yields throughput.
var fsOpBytes = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: namespaceReplistore,
		Subsystem: subsystemFSOp,
		Name:      "bytes_total",
		Help:      "Total bytes transferred across the FUSE boundary, labelled by operation.",
	},
	[]string{"op"},
)

// backendOpBytes counts bytes moved to or from each backend, labelled by
// backend, type, and operation (read or write). A fan-out write counts once per
// replica, so this reflects real per-backend I/O rather than logical bytes.
var backendOpBytes = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: namespaceReplistore,
		Subsystem: subsystemBackend,
		Name:      "bytes_total",
		Help:      "Total bytes transferred to/from backends, labelled by backend, type, and operation.",
	},
	[]string{subsystemBackend, labelType, "op"},
)

// RecordFSOp records a single FUSE operation that began at start: its latency in
// the histogram and a count (labelled by classified error) in the counter.
func RecordFSOp(op string, start time.Time, err error) {
	fsOpDuration.WithLabelValues(op).Observe(elapsed(start))
	fsOpsTotal.WithLabelValues(op, classifyError(err)).Inc()
}

// RecordFSBytes adds n bytes to the FUSE byte counter for op (read or write).
// Non-positive counts are ignored.
func RecordFSBytes(op string, n int) {
	if n <= 0 {
		return
	}
	fsOpBytes.WithLabelValues(op).Add(float64(n))
}

// RecordBackendBytes adds n bytes to the backend byte counter for the given
// backend and op (read or write). Non-positive counts are ignored.
func RecordBackendBytes(backend, backendType, op string, n int) {
	if n <= 0 {
		return
	}
	backendOpBytes.WithLabelValues(backend, backendType, op).Add(float64(n))
}

// RecordBackendPing records the duration of a backend health probe that began
// at start. The outcome is labelled "success" or "error" based on err; backend
// and backendType identify which backend was probed.
func RecordBackendPing(backend, backendType string, start time.Time, err error) {
	backendPingDuration.WithLabelValues(backend, backendType, resultLabel(err)).Observe(elapsed(start))
}

// RecordBackendOp records the duration of a single backend operation that began
// at start. backend and backendType identify the backend, op names the
// operation (e.g. "read", "write", "stat"), and the outcome is labelled
// "success" or "error" based on err.
func RecordBackendOp(backend, backendType, op string, start time.Time, err error) {
	backendOpDuration.WithLabelValues(backend, backendType, op, resultLabel(err)).Observe(elapsed(start))
	backendOpsTotal.WithLabelValues(backend, backendType, op, classifyError(err)).Inc()
}

// elapsed returns seconds since start, clamped at zero against clock skew.
func elapsed(start time.Time) float64 {
	d := time.Since(start).Seconds()
	if d < 0 {
		return 0
	}

	return d
}

// FSMetricsCollector returns the FUSE operation metrics as a Prometheus
// collector, for registration on a registry alongside the other collectors.
// The interface return is deliberate: callers only need the Collector contract.
//
//nolint:ireturn
func FSMetricsCollector() prometheus.Collector {
	return fsOpDuration
}

// BackendMetricsCollector returns the backend ping metrics as a Prometheus
// collector, for registration alongside the other collectors.
//
//nolint:ireturn
func BackendMetricsCollector() prometheus.Collector {
	return backendPingDuration
}

// BackendOpMetricsCollector returns the per-operation backend latency metrics as
// a Prometheus collector, for registration alongside the other collectors.
//
//nolint:ireturn
func BackendOpMetricsCollector() prometheus.Collector {
	return backendOpDuration
}

// FSOpsCounterCollector returns the FUSE operation counters (QPS / error rate).
//
//nolint:ireturn
func FSOpsCounterCollector() prometheus.Collector {
	return fsOpsTotal
}

// BackendOpsCounterCollector returns the backend operation counters (QPS / error rate).
//
//nolint:ireturn
func BackendOpsCounterCollector() prometheus.Collector {
	return backendOpsTotal
}

// FSBytesCounterCollector returns the FUSE byte-throughput counters.
//
//nolint:ireturn
func FSBytesCounterCollector() prometheus.Collector {
	return fsOpBytes
}

// BackendBytesCounterCollector returns the backend byte-throughput counters.
//
//nolint:ireturn
func BackendBytesCounterCollector() prometheus.Collector {
	return backendOpBytes
}

package backend

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/digitalentity/replistore/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBackend embeds the Backend interface (left nil) so only the methods the
// test exercises need real implementations; any other call would panic, which
// keeps the fake honest about what is under test.
type fakeBackend struct {
	Backend
	name    string
	statErr error
}

func (f *fakeBackend) GetName() string { return f.name }
func (f *fakeBackend) GetType() string { return "fake" }

func (f *fakeBackend) Stat(_ context.Context, _ string) (FileInfo, error) {
	return FileInfo{}, f.statErr
}

//nolint:ireturn // backend.File is an interface returned by implementation.
func (f *fakeBackend) OpenFile(_ context.Context, _ string, _ int, _ os.FileMode) (File, error) {
	return &fakeFile{}, nil
}

type fakeFile struct {
	File
}

// ReadAt returns 7 bytes plus io.EOF, the common short-read-at-EOF case.
func (f *fakeFile) ReadAt(_ context.Context, b []byte, _ int64) (int, error) {
	return copy(b, "1234567"), io.EOF
}
func (f *fakeFile) WriteAt(_ context.Context, b []byte, _ int64) (int, error) {
	return len(b), nil
}

func TestMeteredBackend_RecordsOps(t *testing.T) {
	const name = "metered_test_backend"
	fake := &fakeBackend{name: name, statErr: errors.New("boom")}

	m := NewMetered(fake)
	ctx := context.Background()

	_, _ = m.Stat(ctx, "/some/path")
	fh, err := m.OpenFile(ctx, "/some/file", os.O_RDWR, 0)
	require.NoError(t, err)
	_, _ = fh.ReadAt(ctx, make([]byte, 4), 0)
	_, _ = fh.WriteAt(ctx, []byte("data"), 0)

	body := scrapeNamed(t, observability.BackendOpMetricsCollector(),
		"replistore_backend_op_duration_seconds", name)

	// Stat failed: recorded with result="error".
	assert.Contains(t, body,
		`replistore_backend_op_duration_seconds_count{backend="`+name+`",op="stat",result="error",type="fake"} 1`)
	// io.EOF on read must count as success, not error.
	assert.Contains(t, body,
		`replistore_backend_op_duration_seconds_count{backend="`+name+`",op="read",result="success",type="fake"} 1`)
	assert.Contains(t, body,
		`replistore_backend_op_duration_seconds_count{backend="`+name+`",op="write",result="success",type="fake"} 1`)

	// The ops counter carries the classified error label for QPS / error-rate.
	counters := scrapeNamed(t, observability.BackendOpsCounterCollector(),
		"replistore_backend_ops_total", name)
	assert.Contains(t, counters,
		`replistore_backend_ops_total{backend="`+name+`",error="error",op="stat",type="fake"} 1`)
	assert.Contains(t, counters,
		`replistore_backend_ops_total{backend="`+name+`",error="ok",op="read",type="fake"} 1`)
	assert.Contains(t, counters,
		`replistore_backend_ops_total{backend="`+name+`",error="ok",op="write",type="fake"} 1`)

	// Byte counters: 4-byte read (copied into the 4-byte buffer) and 4-byte write.
	bytesOut := scrapeNamed(t, observability.BackendBytesCounterCollector(),
		"replistore_backend_bytes_total", name)
	assert.Contains(t, bytesOut,
		`replistore_backend_bytes_total{backend="`+name+`",op="read",type="fake"} 4`)
	assert.Contains(t, bytesOut,
		`replistore_backend_bytes_total{backend="`+name+`",op="write",type="fake"} 4`)
}

func TestUnwrap(t *testing.T) {
	fake := &fakeBackend{name: "u"}
	m := NewMetered(fake)

	assert.Same(t, fake, Unwrap(m), "Unwrap returns the wrapped backend")
	assert.Same(t, fake, Unwrap(fake), "Unwrap is a no-op on an unwrapped backend")
}

// scrapeNamed collects metricName from c and returns only the lines belonging to
// the given backend.
func scrapeNamed(t *testing.T, c prometheus.Collector, metricName, name string) string {
	t.Helper()
	out, err := testutil.CollectAndFormat(c, expfmt.TypeTextPlain, metricName)
	require.NoError(t, err)

	var b strings.Builder
	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.Contains(line, `backend="`+name+`"`) {
			b.WriteString(line)
			b.WriteString("\n")
		}
	}

	return b.String()
}

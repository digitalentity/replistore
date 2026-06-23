package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordFSOp(t *testing.T) {
	const op = "test_record_op"

	RecordFSOp(op, time.Now().Add(-2*time.Millisecond), nil)
	RecordFSOp(op, time.Now().Add(-2*time.Millisecond), nil)

	count, err := countObservations(op)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "each RecordFSOp call adds one observation")
}

func TestClassifyError(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"nil":        {nil, "ok"},
		"canceled":   {context.Canceled, "canceled"},
		"timeout":    {context.DeadlineExceeded, "timeout"},
		"not_found":  {os.ErrNotExist, "not_found"},
		"permission": {os.ErrPermission, "permission"},
		"exists":     {os.ErrExist, "exists"},
		"eof":        {io.EOF, "eof"},
		"wrapped":    {fmt.Errorf("open: %w", os.ErrNotExist), "not_found"},
		"unknown":    {errors.New("boom"), "error"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyError(tc.err))
		})
	}
}

func TestRecordFSOp_CountsErrorLabel(t *testing.T) {
	const op = "test_err_op"
	RecordFSOp(op, time.Now(), os.ErrNotExist)

	out, err := testutil.CollectAndFormat(FSOpsCounterCollector(),
		expfmt.TypeTextPlain, "replistore_fsop_ops_total")
	require.NoError(t, err)
	assert.Contains(t, string(out),
		`replistore_fsop_ops_total{error="not_found",op="`+op+`"} 1`)
}

func TestRecordBackendPing(t *testing.T) {
	const name = "test_ping_backend"

	RecordBackendPing(name, "mock", time.Now().Add(-1*time.Millisecond), nil)
	RecordBackendPing(name, "mock", time.Now().Add(-1*time.Millisecond), assert.AnError)

	out, err := testutil.CollectAndFormat(BackendMetricsCollector(),
		expfmt.TypeTextPlain, "replistore_backend_ping_duration_seconds")
	require.NoError(t, err)
	body := string(out)

	// Outcome is split by the result label; name and type are carried too.
	assert.Contains(t, body, `replistore_backend_ping_duration_seconds_count{backend="`+name+`",result="success",type="mock"} 1`)
	assert.Contains(t, body, `replistore_backend_ping_duration_seconds_count{backend="`+name+`",result="error",type="mock"} 1`)
}

// countObservations returns the histogram _count for the given op label by
// scraping the exposition output of the FS metrics collector.
func countObservations(op string) (int, error) {
	out, err := testutil.CollectAndFormat(FSMetricsCollector(),
		expfmt.TypeTextPlain, "replistore_fsop_duration_seconds")
	if err != nil {
		return 0, err
	}

	want := `replistore_fsop_duration_seconds_count{op="` + op + `"} `
	for line := range strings.SplitSeq(string(out), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), want); ok {
			return strconv.Atoi(rest)
		}
	}

	return 0, nil
}

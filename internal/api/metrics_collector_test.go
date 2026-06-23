package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/digitalentity/replistore/internal/backend"
	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_HandleMetrics(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("GetTotalSpace").Return(uint64(3000), nil)
	b1.On("GetFreeSpace").Return(uint64(1500), nil)

	b2 := &bmock.MockBackend{NameVal: "b2"}
	b2.On("GetTotalSpace").Return(uint64(6000), nil)
	b2.On("GetFreeSpace").Return(uint64(3000), nil)

	backends := map[string]backend.Backend{"b1": b1, "b2": b2}

	cache := vfs.NewCache()
	cache.Upsert("file1.txt", vfs.Metadata{Name: "file1.txt", Size: 500, IsDir: false}, "b1")

	replFS := &fuse.RepliFS{
		Backends:          backends,
		ReplicationFactor: 2,
		Cache:             cache,
	}

	server := NewServer("", "", "", replFS, nil, "", "v0.1.0")

	ctx := context.Background()
	req := httptest.NewRequestWithContext(ctx, "GET", "/streamz", nil)
	w := httptest.NewRecorder()

	server.handleMetrics(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	body := string(bodyBytes)

	// Instance and build metrics.
	assert.Contains(t, body, "replistore_up 1")
	assert.Contains(t, body, `replistore_build_info{version="v0.1.0"} 1`)

	// Per-backend gauges carry name and type labels (HealthMonitor nil means healthy).
	assert.Contains(t, body, `replistore_backend_up{backend="b1",type="mock"} 1`)
	assert.Contains(t, body, `replistore_backend_total_bytes{backend="b2",type="mock"} 6000`)

	// Aggregated cluster space and replication factor.
	assert.Contains(t, body, "replistore_cluster_raw_total_bytes 9000")
	assert.Contains(t, body, "replistore_cluster_raw_free_bytes 4500")
	assert.Contains(t, body, "replistore_replication_factor 2")

	// Cache counters.
	assert.Contains(t, body, "replistore_cache_hits_total")
	assert.Contains(t, body, "replistore_cache_misses_total")

	// Go runtime collectors are wired in too.
	assert.Contains(t, body, "go_goroutines")
}

func TestServer_HandleMetrics_NoSpaceUnderflow(t *testing.T) {
	// A backend reporting free > total must not underflow raw_used (#1).
	b := &bmock.MockBackend{NameVal: "b1"}
	b.On("GetTotalSpace").Return(uint64(100), nil)
	b.On("GetFreeSpace").Return(uint64(200), nil)

	replFS := &fuse.RepliFS{
		Backends:          map[string]backend.Backend{"b1": b},
		ReplicationFactor: 1,
		Cache:             vfs.NewCache(),
	}
	server := NewServer("", "", "", replFS, nil, "", "v0.1.0")

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/streamz", nil)
	w := httptest.NewRecorder()
	server.handleMetrics(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	// Used clamps to 0 instead of wrapping to a near-MaxUint64 value.
	assert.Contains(t, string(body), "replistore_cluster_raw_used_bytes 0")
}

func TestServer_HandleMetrics_SkipsUnhealthyForSpace(t *testing.T) {
	// A backend whose space calls would error must be skipped, not summed (#1/#2).
	healthy := &bmock.MockBackend{NameVal: "ok"}
	healthy.On("GetTotalSpace").Return(uint64(1000), nil)
	healthy.On("GetFreeSpace").Return(uint64(400), nil)

	broken := &bmock.MockBackend{NameVal: "broken"}
	broken.On("GetTotalSpace").Return(uint64(0), errors.New("statfs timeout"))
	broken.On("GetFreeSpace").Return(uint64(0), errors.New("statfs timeout"))

	replFS := &fuse.RepliFS{
		Backends:          map[string]backend.Backend{"ok": healthy, "broken": broken},
		ReplicationFactor: 1,
		Cache:             vfs.NewCache(),
	}
	server := NewServer("", "", "", replFS, nil, "", "v0.1.0")

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/streamz", nil)
	w := httptest.NewRecorder()
	server.handleMetrics(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	// Only the healthy backend contributes: total 1000, free 400, used 600.
	assert.Contains(t, string(body), "replistore_cluster_raw_total_bytes 1000")
	assert.Contains(t, string(body), "replistore_cluster_raw_used_bytes 600")
}

func TestServer_HandleMetrics_RepairAlwaysPresent(t *testing.T) {
	replFS := &fuse.RepliFS{
		Backends:          map[string]backend.Backend{},
		ReplicationFactor: 1,
		Cache:             vfs.NewCache(),
	}
	server := NewServer("", "", "", replFS, nil, "", "v0.1.0")

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/streamz", nil)
	w := httptest.NewRecorder()
	server.handleMetrics(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	// With no RepairManager the repair series must still be present and zeroed,
	// so alerting can tell "healthy" apart from "not scraped".
	assert.Contains(t, string(body), "replistore_repair_scrub_active 0")
	assert.Contains(t, string(body), "replistore_repair_degraded_files 0")
}

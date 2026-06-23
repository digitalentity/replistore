package api

import (
	"context"
	"encoding/json"
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

func TestBearerTokenMatch(t *testing.T) {
	const want = "s3cret-token"
	cases := []struct {
		name   string
		header string
		match  bool
	}{
		{"exact match", "Bearer s3cret-token", true},
		{"wrong token", "Bearer wrong-token", false},
		{"missing prefix", "s3cret-token", false},
		{"empty header", "", false},
		{"prefix only", "Bearer ", false},
		{"token is a prefix of want", "Bearer s3cret", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.match, bearerTokenMatch(tc.header, want))
		})
	}
}

func TestServer_APIAuthHandler(t *testing.T) {
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

	t.Run("no token configured allows request", func(t *testing.T) {
		s := &Server{}
		h := s.apiAuthHandler(http.HandlerFunc(ok))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), "GET", "/api/health", nil))
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("token configured rejects missing header", func(t *testing.T) {
		s := &Server{apiToken: "tok"}
		h := s.apiAuthHandler(http.HandlerFunc(ok))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), "GET", "/api/health", nil))
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("token configured accepts matching header", func(t *testing.T) {
		s := &Server{apiToken: "tok"}
		h := s.apiAuthHandler(http.HandlerFunc(ok))
		req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/health", nil)
		req.Header.Set("Authorization", "Bearer tok")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestServer_HandleClusterStats(t *testing.T) {
	// Mock backends
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("GetTotalSpace").Return(uint64(3000), nil)
	b1.On("GetFreeSpace").Return(uint64(1500), nil)

	b2 := &bmock.MockBackend{NameVal: "b2"}
	b2.On("GetTotalSpace").Return(uint64(6000), nil)
	b2.On("GetFreeSpace").Return(uint64(3000), nil)

	backends := map[string]backend.Backend{
		"b1": b1,
		"b2": b2,
	}

	cache := vfs.NewCache()
	cache.Upsert("file1.txt", vfs.Metadata{Name: "file1.txt", Size: 500, IsDir: false}, "b1")

	replFS := &fuse.RepliFS{
		Backends:          backends,
		ReplicationFactor: 2,
		Cache:             cache,
	}

	server := NewServer("", "", "", replFS, nil, "", "v0.1.0")

	ctx := context.Background()
	req := httptest.NewRequestWithContext(ctx, "GET", "/api/cluster/stats", nil)
	w := httptest.NewRecorder()

	server.handleClusterStats(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var stats struct {
		Raw struct {
			Total uint64 `json:"total_space_bytes"`
			Used  uint64 `json:"used_space_bytes"`
			Free  uint64 `json:"free_space_bytes"`
		} `json:"raw"`
		Amortized struct {
			Total uint64 `json:"total_space_bytes"`
			Used  uint64 `json:"used_space_bytes"`
			Free  uint64 `json:"free_space_bytes"`
		} `json:"amortized"`
		RF uint64 `json:"replication_factor"`
	}

	err := json.NewDecoder(resp.Body).Decode(&stats)

	require.NoError(t, err)

	// Validate response
	assert.Equal(t, uint64(9000), stats.Raw.Total) // 3000 + 6000
	assert.Equal(t, uint64(4500), stats.Raw.Free)  // 1500 + 3000
	assert.Equal(t, uint64(4500), stats.Raw.Used)  // 9000 - 4500

	assert.Equal(t, uint64(4500), stats.Amortized.Total) // 9000 / 2
	assert.Equal(t, uint64(2250), stats.Amortized.Free)  // 4500 / 2
	assert.Equal(t, uint64(500), stats.Amortized.Used)   // Logical file size from cache

	assert.Equal(t, uint64(2), stats.RF)
}

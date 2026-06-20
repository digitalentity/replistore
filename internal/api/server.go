// Package api implements the REST API and control server for RepliStore.
//
//nolint:goconst
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/backend/local"
	"github.com/digitalentity/replistore/internal/backend/smb"
	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/vfs"
	"golang.org/x/sync/errgroup"
)

const (
	defaultReadHeaderTimeout = 10 * time.Second
	bufferSize64K            = 64 * 1024
	shutdownDelay            = 500 * time.Millisecond
)

//nolint:unparam
func (s *Server) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("Failed to encode JSON response", slog.Any("error", err))
	}
}

// Server implements the HTTP API server.
type Server struct {
	addr         string
	apiToken     string
	metricsToken string
	replFS       *fuse.RepliFS
	repairMgr    *fuse.RepairManager
	configPath   string
	version      string
	startTime    time.Time
	httpServer   *http.Server
}

// NewServer creates a new API Server instance.
func NewServer(addr, apiToken, metricsToken string, replFS *fuse.RepliFS, repairMgr *fuse.RepairManager, configPath string, version string) *Server {
	return &Server{
		addr:         addr,
		apiToken:     apiToken,
		metricsToken: metricsToken,
		replFS:       replFS,
		repairMgr:    repairMgr,
		configPath:   configPath,
		version:      version,
		startTime:    time.Now(),
	}
}

// Start starts the HTTP server and registers all API routes.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Register /api/ routes
	mux.Handle("GET /api/health", s.apiAuthHandler(http.HandlerFunc(s.handleHealth)))
	mux.Handle("GET /api/backends", s.apiAuthHandler(http.HandlerFunc(s.handleBackends)))
	mux.Handle("GET /api/cluster/peers", s.apiAuthHandler(http.HandlerFunc(s.handleClusterPeers)))
	mux.Handle("GET /api/cluster/locks", s.apiAuthHandler(http.HandlerFunc(s.handleClusterLocks)))
	mux.Handle("GET /api/cache/stats", s.apiAuthHandler(http.HandlerFunc(s.handleCacheStats)))
	mux.Handle("POST /api/cache/refresh", s.apiAuthHandler(http.HandlerFunc(s.handleCacheRefresh)))
	mux.Handle("GET /api/repair/status", s.apiAuthHandler(http.HandlerFunc(s.handleRepairStatus)))

	// File Management routes (using path wildcards)
	mux.Handle("GET /api/meta/{path...}", s.apiAuthHandler(http.HandlerFunc(s.handleMeta)))
	mux.Handle("GET /api/data/{path...}", s.apiAuthHandler(http.HandlerFunc(s.handleData)))
	mux.Handle("PUT /api/data/{path...}", s.apiAuthHandler(http.HandlerFunc(s.handleData)))
	mux.Handle("DELETE /api/data/{path...}", s.apiAuthHandler(http.HandlerFunc(s.handleData)))

	// Node shutdown
	mux.Handle("POST /api/shutdown", s.apiAuthHandler(http.HandlerFunc(s.handleShutdown)))

	// Register /streamz endpoints
	mux.Handle("GET /streamz", s.metricsAuthHandler(http.HandlerFunc(s.handleMetrics)))

	s.httpServer = &http.Server{
		Addr:              s.addr,
		Handler:           s.loggingMiddleware(mux),
		ReadHeaderTimeout: defaultReadHeaderTimeout,
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", s.addr)
	if err != nil {
		return err
	}

	slog.Info("HTTP server listening", slog.String("addr", ln.Addr().String()))

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", slog.Any("error", err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer != nil {
		slog.Info("Shutting down HTTP server")

		return s.httpServer.Shutdown(ctx)
	}

	return nil
}

func (s *Server) apiAuthHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiToken != "" {
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") || strings.TrimPrefix(authHeader, "Bearer ") != s.apiToken {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)

				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) metricsAuthHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.metricsToken != "" {
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") || strings.TrimPrefix(authHeader, "Bearer ") != s.metricsToken {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)

				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

type responseWriterWrapper struct {
	http.ResponseWriter
	status int
}

func (w *responseWriterWrapper) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriterWrapper{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		slog.Info("HTTP Request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", wrapped.status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	res := map[string]any{
		"status":         "healthy",
		"version":        s.version,
		"uptime_seconds": int64(time.Since(s.startTime).Seconds()),
	}
	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) getBackendList() []backend.Backend {
	list := make([]backend.Backend, 0, len(s.replFS.Backends))
	for _, b := range s.replFS.Backends {
		list = append(list, b)
	}

	return list
}

func (s *Server) handleBackends(w http.ResponseWriter, r *http.Request) {
	result := make([]map[string]any, 0, len(s.replFS.Backends))
	for name, b := range s.replFS.Backends {
		addr := ""
		if smbB, ok := b.(*smb.SMBBackend); ok {
			addr = smbB.Address
		} else if localB, ok := b.(*local.LocalBackend); ok {
			addr = localB.Path
		}

		healthy := true
		var latencyMs int64 = -1
		var lastErr string

		if s.replFS.HealthMonitor != nil {
			healthy = s.replFS.HealthMonitor.IsHealthy(name)
			if healthy {
				latencyMs = s.replFS.HealthMonitor.GetLatency(name).Milliseconds()
			} else if err := s.replFS.HealthMonitor.GetLastError(name); err != nil {
				lastErr = err.Error()
			}
		}

		var freeSpace uint64
		var totalSpace uint64
		if healthy {
			freeSpace, _ = b.GetFreeSpace()
			totalSpace, _ = b.GetTotalSpace()
		}

		info := map[string]any{
			"name":              name,
			"address":           addr,
			"healthy":           healthy,
			"latency_ms":        latencyMs,
			"free_space_bytes":  freeSpace,
			"total_space_bytes": totalSpace,
		}
		if !healthy && lastErr != "" {
			info["last_error"] = lastErr
		}
		result = append(result, info)
	}
	s.writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleClusterPeers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	expectedSize := 1
	var peers []cluster.PeerStatus
	if s.replFS.Discovery != nil {
		peers = s.replFS.Discovery.GetPeersStatus()
	}

	if s.replFS.LockManager != nil {
		expectedSize = s.replFS.LockManager.ExpectedClusterSize
	}

	currentSize := len(peers) + 1

	res := map[string]any{
		"expected_cluster_size": expectedSize,
		"current_cluster_size":  currentSize,
		"peers":                 peers,
	}
	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleClusterLocks(w http.ResponseWriter, r *http.Request) {
	var locks []cluster.LockInfo
	if s.replFS.LockManager != nil {
		locks = s.replFS.LockManager.GetActiveLocks()
	}
	if locks == nil {
		locks = []cluster.LockInfo{}
	}
	s.writeJSON(w, http.StatusOK, locks)
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	totalNodes, fullyIndexedDirs := s.replFS.Cache.GetStats()
	res := map[string]any{
		"total_cached_nodes":        totalNodes,
		"directories_fully_indexed": fullyIndexedDirs,
		"cache_hits":                s.replFS.Cache.Hits.Load(),
		"cache_misses":              s.replFS.Cache.Misses.Load(),
	}
	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleCacheRefresh(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		http.Error(w, "Bad Request", http.StatusBadRequest)

		return
	}

	cleanPath := strings.Trim(req.Path, "/")
	if cleanPath == "" {
		//nolint:contextcheck
		go s.replFS.Cache.Warmup(context.Background(), s.getBackendList())
		res := map[string]any{
			"status": "refresh_queued",
			"path":   "/",
		}
		s.writeJSON(w, http.StatusOK, res)
	} else {
		//nolint:contextcheck
		go func() {
			node, err := s.replFS.Cache.FetchEntry(context.Background(), cleanPath, s.getBackendList())
			if err == nil && node != nil {
				node.Mu.RLock()
				isDir := node.Meta.IsDir
				node.Mu.RUnlock()
				if isDir {
					_ = s.replFS.Cache.FetchDir(context.Background(), cleanPath, s.getBackendList())
				}
			}
		}()
		res := map[string]any{
			"status": "refresh_queued",
			"path":   "/" + cleanPath,
		}
		s.writeJSON(w, http.StatusOK, res)
	}
}

func (s *Server) handleRepairStatus(w http.ResponseWriter, r *http.Request) {
	if s.repairMgr == nil {
		s.writeJSON(w, http.StatusOK, map[string]any{
			"scrub_active":                false,
			"last_scrub_duration_seconds": 0.0,
			"degraded_files_count":        0,
			"divergent_files_count":       0,
			"active_repairs":              []any{},
		})

		return
	}
	status := s.repairMgr.GetStatus()
	s.writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := r.PathValue("path")
	cleanPath := strings.Trim(path, "/")

	node, ok := s.replFS.Cache.Get(cleanPath)
	if !ok {
		http.Error(w, "Not Found", http.StatusNotFound)

		return
	}

	node.Mu.RLock()
	isDir := node.Meta.IsDir
	meta := node.Meta

	var children []map[string]any
	if isDir {
		for _, child := range node.Children {
			child.Mu.RLock()
			cMeta := child.Meta
			child.Mu.RUnlock()
			children = append(children, map[string]any{
				"name":          cMeta.Name,
				"is_dir":        cMeta.IsDir,
				"size_bytes":    cMeta.Size,
				"modified_time": cMeta.ModTime.Format(time.RFC3339),
				"generation":    cMeta.DataGen,
			})
		}
	}
	node.Mu.RUnlock()

	displayPath := "/" + cleanPath
	if cleanPath == "" {
		displayPath = "/"
	}

	if isDir {
		res := map[string]any{
			"name":     meta.Name,
			"path":     displayPath,
			"is_dir":   true,
			"children": children,
		}
		if meta.Name == "/" || meta.Name == "" {
			res["name"] = "/"
		}
		s.writeJSON(w, http.StatusOK, res)
	} else {
		res := map[string]any{
			"name":          meta.Name,
			"path":          displayPath,
			"is_dir":        false,
			"size_bytes":    meta.Size,
			"modified_time": meta.ModTime.Format(time.RFC3339),
			"generation":    meta.DataGen,
		}
		s.writeJSON(w, http.StatusOK, res)
	}
}

//nolint:containedctx
type readerWrapper struct {
	ctx context.Context
	f   backend.File
	off int64
}

func (rw *readerWrapper) Read(b []byte) (int, error) {
	n, err := rw.f.ReadAt(rw.ctx, b, rw.off)
	rw.off += int64(n)

	return n, err
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	path := r.PathValue("path")
	cleanPath := strings.Trim(path, "/")

	switch r.Method {
	case http.MethodGet:
		s.handleDataGet(w, r, cleanPath)
	case http.MethodPut:
		s.handleDataPut(w, r, cleanPath)
	case http.MethodDelete:
		s.handleDataDelete(w, r, cleanPath)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDataGet(w http.ResponseWriter, r *http.Request, cleanPath string) {
	node, ok := s.replFS.Cache.Get(cleanPath)
	if !ok {
		http.Error(w, "Not Found", http.StatusNotFound)

		return
	}

	node.Mu.RLock()
	isDir := node.Meta.IsDir
	meta := node.Meta
	node.Mu.RUnlock()

	if isDir {
		http.Error(w, "Cannot read raw bytes of a directory", http.StatusBadRequest)

		return
	}

	bName := s.replFS.Selector.SelectForRead(meta)
	if bName == "" {
		http.Error(w, "No healthy backends available for read", http.StatusServiceUnavailable)

		return
	}

	b := s.replFS.Backends[bName]
	sf, err := b.OpenFile(r.Context(), cleanPath, os.O_RDONLY, 0)
	if err != nil {
		slog.Error("Failed to open file on backend", slog.String("backend", bName), slog.String("path", cleanPath), slog.Any("error", err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)

		return
	}
	defer sf.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))

	rw := &readerWrapper{ctx: r.Context(), f: sf, off: 0}
	_, _ = io.Copy(w, rw)
}

func (s *Server) handleDataPut(w http.ResponseWriter, r *http.Request, cleanPath string) {
	if cleanPath == "" {
		http.Error(w, "Empty file path", http.StatusBadRequest)

		return
	}

	w.Header().Set("Content-Type", "application/json")

	var writeBackends []string
	var oldGen int64
	node, ok := s.replFS.Cache.Get(cleanPath)
	if ok {
		node.Mu.RLock()
		isDir := node.Meta.IsDir
		if isDir {
			node.Mu.RUnlock()
			http.Error(w, "Path is an existing directory", http.StatusBadRequest)

			return
		}
		writeBackends = make([]string, len(node.Meta.Backends))
		copy(writeBackends, node.Meta.Backends)
		oldGen = node.Meta.DataGen
		node.Mu.RUnlock()
	}

	if len(writeBackends) == 0 {
		allBackendNames := s.replFS.AllBackendNames()
		writeBackends = s.replFS.Selector.SelectForWrite(s.replFS.ReplicationFactor, allBackendNames)
	}

	if len(writeBackends) == 0 {
		http.Error(w, "No backends available for write", http.StatusServiceUnavailable)

		return
	}

	tmpFile, err := os.CreateTemp(".", "replistore-upload-")
	if err != nil {
		http.Error(w, "Failed to create temp file", http.StatusInternalServerError)

		return
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	size, err := io.Copy(tmpFile, r.Body)
	if err != nil {
		http.Error(w, "Failed to read upload body", http.StatusInternalServerError)

		return
	}

	var mu sync.Mutex
	var successfulBackends []string
	g, gCtx := errgroup.WithContext(r.Context())

	for _, bName := range writeBackends {
		g.Go(func() error {
			b := s.replFS.Backends[bName]
			parentPath := filepath.Dir(cleanPath)
			if parentPath != "." && parentPath != "/" && parentPath != "" {
				_ = b.MkdirAll(gCtx, parentPath, 0755)
			}
			sf, err := b.OpenFile(gCtx, cleanPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			defer sf.Close()

			buf := make([]byte, bufferSize64K)
			var offset int64
			for {
				n, readErr := tmpFile.ReadAt(buf, offset)
				if n > 0 {
					_, writeErr := sf.WriteAt(gCtx, buf[:n], offset)
					if writeErr != nil {
						return writeErr
					}
					offset += int64(n)
				}
				if readErr != nil {
					if readErr == io.EOF {
						break
					}

					return readErr
				}
			}

			mu.Lock()
			successfulBackends = append(successfulBackends, bName)
			mu.Unlock()

			return nil
		})
	}
	_ = g.Wait()

	if len(successfulBackends) < s.replFS.WriteQuorum {
		for _, bName := range successfulBackends {
			_ = s.replFS.Backends[bName].Remove(r.Context(), cleanPath)
		}
		http.Error(w, "Write quorum not met", http.StatusServiceUnavailable)

		return
	}

	newGen := max(oldGen, s.replFS.MaxTombstoneGen(r.Context(), cleanPath)) + 1
	s.replFS.WriteSidecars(r.Context(), cleanPath, vfs.Sidecar{DataGen: newGen, Writer: s.replFS.NodeID}, successfulBackends)

	s.replFS.Cache.Upsert(cleanPath, vfs.Metadata{
		Name:     filepath.Base(cleanPath),
		Path:     cleanPath,
		Size:     size,
		Mode:     0644,
		ModTime:  time.Now(),
		IsDir:    false,
		Backends: successfulBackends,
		DataGen:  newGen,
	}, successfulBackends[0])

	for _, bName := range successfulBackends[1:] {
		s.replFS.Cache.Upsert(cleanPath, vfs.Metadata{
			Name:     filepath.Base(cleanPath),
			Path:     cleanPath,
			Size:     size,
			Mode:     0644,
			ModTime:  time.Now(),
			IsDir:    false,
			Backends: successfulBackends,
			DataGen:  newGen,
		}, bName)
	}

	res := map[string]any{
		"status":           "uploaded",
		"path":             "/" + cleanPath,
		"written_replicas": len(successfulBackends),
		"generation":       newGen,
	}
	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleDataDelete(w http.ResponseWriter, r *http.Request, cleanPath string) {
	w.Header().Set("Content-Type", "application/json")
	if cleanPath == "" {
		http.Error(w, "Empty path", http.StatusBadRequest)

		return
	}

	node, ok := s.replFS.Cache.Get(cleanPath)
	if !ok {
		http.Error(w, "Not Found", http.StatusNotFound)

		return
	}

	node.Mu.RLock()
	isDir := node.Meta.IsDir
	gen := node.Meta.DataGen
	backends := make([]string, len(node.Meta.Backends))
	copy(backends, node.Meta.Backends)
	node.Mu.RUnlock()

	if isDir {
		_ = s.replFS.Cache.FetchDir(r.Context(), cleanPath, s.getBackendList())
		node.Mu.RLock()
		hasChildren := len(node.Children) > 0
		node.Mu.RUnlock()
		if hasChildren {
			http.Error(w, "Directory not empty", http.StatusBadRequest)

			return
		}
		backends = s.replFS.AllBackendNames()
	}

	tombGen := gen + 1
	tomb := vfs.Sidecar{DataGen: tombGen, Writer: s.replFS.NodeID, Deleted: true}
	successes := s.replFS.WriteTombstones(r.Context(), cleanPath, tomb, s.replFS.AllBackendNames())
	if successes < s.replFS.WriteQuorum {
		http.Error(w, "Could not reach tombstone write quorum", http.StatusServiceUnavailable)

		return
	}

	g, gCtx := errgroup.WithContext(r.Context())
	for _, bName := range backends {
		g.Go(func() error {
			b := s.replFS.Backends[bName]
			err := b.Remove(gCtx, cleanPath)
			if err != nil && !os.IsNotExist(err) && !errors.Is(err, os.ErrNotExist) {
				return err
			}

			return nil
		})
	}
	_ = g.Wait()

	s.replFS.Cache.Evict(cleanPath)

	res := map[string]any{
		"status": "deleted",
		"path":   "/" + cleanPath,
	}
	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status": "shutting_down",
	})

	go func() {
		time.Sleep(shutdownDelay)
		p, err := os.FindProcess(os.Getpid())
		if err == nil {
			_ = p.Signal(os.Interrupt) // send SIGINT/Interrupt to trigger mount unmount loop
		}
	}()
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# HELP replistore_up Status of RepliStore instance\n# TYPE replistore_up gauge\nreplistore_up 1\n")
}

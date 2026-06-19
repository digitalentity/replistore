// Package api implements the minimal HTTP server scaffolding for the control and metrics endpoints.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// APIResponse represents the JSON payload returned by API endpoints.
type APIResponse struct {
	Endpoint string `json:"endpoint"`
	Message  string `json:"message"`
	Time     string `json:"time"`
}

const defaultReadHeaderTimeout = 10 * time.Second

// Server implements the HTTP API server.
type Server struct {
	addr         string
	apiToken     string
	metricsToken string
	httpServer   *http.Server
}

// NewServer creates a new API Server instance.
func NewServer(addr, apiToken, metricsToken string) *Server {
	return &Server{
		addr:         addr,
		apiToken:     apiToken,
		metricsToken: metricsToken,
	}
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Register /api/ endpoints with apiToken auth middleware
	mux.Handle("/api/", s.apiAuthHandler(http.HandlerFunc(s.handleAPI)))

	// Register /streamz endpoints with metricsToken auth middleware
	mux.Handle("/streamz", s.metricsAuthHandler(http.HandlerFunc(s.handleMetrics)))
	mux.Handle("/streamz/", s.metricsAuthHandler(http.HandlerFunc(s.handleMetrics)))

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

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := APIResponse{
		Endpoint: r.URL.Path,
		Message:  "minimal http server scaffolding",
		Time:     time.Now().Format(time.RFC3339),
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode JSON response", slog.Any("error", err))
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# HELP replistore_up Status of RepliStore instance\n# TYPE replistore_up gauge\nreplistore_up 1\n")
}

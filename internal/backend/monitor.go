package backend

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const pingTimeout = 2 * time.Second

type HealthMonitor struct {
	backends   map[string]Backend
	status     map[string]bool
	latencies  map[string]time.Duration
	lastErrors map[string]error
	mu         sync.RWMutex
	log        *slog.Logger
}

func NewHealthMonitor(backends map[string]Backend) *HealthMonitor {
	status := make(map[string]bool)
	latencies := make(map[string]time.Duration)
	lastErrors := make(map[string]error)
	for name := range backends {
		status[name] = true
		latencies[name] = -1
	}

	return &HealthMonitor{
		backends:   backends,
		status:     status,
		latencies:  latencies,
		lastErrors: lastErrors,
		mu:         sync.RWMutex{},
		log:        slog.Default().With(slog.String("component", "health-monitor")),
	}
}

func (m *HealthMonitor) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.checkAll(ctx)
			}
		}
	}()
}

func (m *HealthMonitor) checkAll(ctx context.Context) {
	g, gCtx := errgroup.WithContext(ctx)

	for name, b := range m.backends {
		g.Go(func() error {
			// Each ping gets its own sub-timeout
			pingCtx, cancel := context.WithTimeout(gCtx, pingTimeout)
			defer cancel()

			start := time.Now()
			err := b.Ping(pingCtx)
			dur := time.Since(start)

			m.mu.Lock()
			if err != nil {
				if m.status[name] {
					m.log.Warn("Backend is DOWN", slog.String("backend", name), slog.Any("error", err))
				}
				m.status[name] = false
				m.latencies[name] = -1
				m.lastErrors[name] = err
			} else {
				if !m.status[name] {
					m.log.Info("Backend is UP", slog.String("backend", name))
				}
				m.status[name] = true
				m.latencies[name] = dur
				m.lastErrors[name] = nil
			}
			m.mu.Unlock()

			return nil
		})
	}
	_ = g.Wait()
}

func (m *HealthMonitor) IsHealthy(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.status[name]
}

func (m *HealthMonitor) GetLatency(name string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.latencies[name]
}

func (m *HealthMonitor) GetLastError(name string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastErrors[name]
}

func (m *HealthMonitor) GetHealthyBackends() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []string
	for name, healthy := range m.status {
		if healthy {
			res = append(res, name)
		}
	}

	return res
}

package backend

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type HealthMonitor struct {
	backends map[string]Backend
	status   map[string]bool
	mu       sync.RWMutex
}

func NewHealthMonitor(backends map[string]Backend) *HealthMonitor {
	status := make(map[string]bool)
	for name := range backends {
		status[name] = true
	}
	return &HealthMonitor{
		backends: backends,
		status:   status,
		mu:       sync.RWMutex{},
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
		name, b := name, b
		g.Go(func() error {
			// Each ping gets its own sub-timeout
			pingCtx, cancel := context.WithTimeout(gCtx, 2*time.Second)
			defer cancel()
			
			err := b.Ping(pingCtx)
			
			m.mu.Lock()
			if err != nil {
				if m.status[name] {
					logrus.Warnf("Backend %s is DOWN: %v", name, err)
				}
				m.status[name] = false
			} else {
				if !m.status[name] {
					logrus.Infof("Backend %s is UP", name)
				}
				m.status[name] = true
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

package backend

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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
	}
}

func (m *HealthMonitor) Start(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			m.checkAll()
		}
	}()
}

func (m *HealthMonitor) checkAll() {
	for name, b := range m.backends {
		err := b.Ping()
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
	}
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

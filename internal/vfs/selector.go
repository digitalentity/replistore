package vfs

import (
	"math/rand"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
)

type BackendSelector interface {
	SelectForRead(meta Metadata) string
	SelectForWrite(count int, allBackends []string) []string
}

type RandomSelector struct {
	r       *rand.Rand
	monitor *backend.HealthMonitor
}

func NewRandomSelector(monitor *backend.HealthMonitor) *RandomSelector {
	return &RandomSelector{
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor: monitor,
	}
}

func (s *RandomSelector) SelectForRead(meta Metadata) string {
	var healthyBackends []string
	for _, b := range meta.Backends {
		if s.monitor == nil || s.monitor.IsHealthy(b) {
			healthyBackends = append(healthyBackends, b)
		}
	}

	if len(healthyBackends) == 0 {
		return ""
	}
	return healthyBackends[s.r.Intn(len(healthyBackends))]
}

func (s *RandomSelector) SelectForWrite(count int, allBackends []string) []string {
	if count <= 0 || len(allBackends) == 0 {
		return nil
	}

	var healthyBackends []string
	for _, b := range allBackends {
		if s.monitor == nil || s.monitor.IsHealthy(b) {
			healthyBackends = append(healthyBackends, b)
		}
	}

	if len(healthyBackends) == 0 {
		return nil
	}

	if count >= len(healthyBackends) {
		return healthyBackends
	}

	perm := s.r.Perm(len(healthyBackends))
	res := make([]string, count)
	for i := 0; i < count; i++ {
		res[i] = healthyBackends[perm[i]]
	}
	return res
}

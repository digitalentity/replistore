package vfs

import (
	"math/rand"
	"slices"
	"sort"
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

type FirstSelector struct {
	monitor *backend.HealthMonitor
}

func NewFirstSelector(monitor *backend.HealthMonitor) *FirstSelector {
	return &FirstSelector{monitor: monitor}
}

func (s *FirstSelector) SelectForRead(meta Metadata) string {
	for _, b := range meta.Backends {
		if s.monitor == nil || s.monitor.IsHealthy(b) {
			return b
		}
	}
	return ""
}

func (s *FirstSelector) SelectForWrite(count int, allBackends []string) []string {
	var res []string
	for _, b := range allBackends {
		if s.monitor == nil || s.monitor.IsHealthy(b) {
			res = append(res, b)
			if len(res) == count {
				break
			}
		}
	}
	return res
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
	for i := range count {
		res[i] = healthyBackends[perm[i]]
	}
	return res
}

type SpaceAwareSelector struct {
	r             *rand.Rand
	monitor       *backend.HealthMonitor
	backends      map[string]backend.Backend
	writeAffinity []string
}

func NewSpaceAwareSelector(backends map[string]backend.Backend, monitor *backend.HealthMonitor, writeAffinity []string) *SpaceAwareSelector {
	return &SpaceAwareSelector{
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		monitor:       monitor,
		backends:      backends,
		writeAffinity: writeAffinity,
	}
}

func (s *SpaceAwareSelector) hasAffinityTag(b backend.Backend) bool {
	if b == nil {
		return false
	}
	tags := b.GetTags()
	for _, t := range tags {
		if slices.Contains(s.writeAffinity, t) {
			return true
		}
	}
	return false
}

func (s *SpaceAwareSelector) getFreeSpace(name string) uint64 {
	b, ok := s.backends[name]
	if !ok {
		return 0
	}
	space, err := b.GetFreeSpace()
	if err != nil {
		return 0
	}
	return space
}

func (s *SpaceAwareSelector) SelectForRead(meta Metadata) string {
	var candidates []string
	for _, b := range meta.Backends {
		if s.monitor == nil || s.monitor.IsHealthy(b) {
			candidates = append(candidates, b)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	maxSpeed := -1
	for _, name := range candidates {
		speed := 10
		if b, ok := s.backends[name]; ok {
			speed = b.GetSpeed()
		}
		if speed > maxSpeed {
			maxSpeed = speed
		}
	}

	var fastest []string
	for _, name := range candidates {
		speed := 10
		if b, ok := s.backends[name]; ok {
			speed = b.GetSpeed()
		}
		if speed == maxSpeed {
			fastest = append(fastest, name)
		}
	}

	if len(fastest) == 0 {
		return candidates[s.r.Intn(len(candidates))]
	}
	return fastest[s.r.Intn(len(fastest))]
}

func (s *SpaceAwareSelector) SelectForWrite(count int, allBackends []string) []string {
	if count <= 0 || len(allBackends) == 0 {
		return nil
	}

	var healthy []string
	for _, name := range allBackends {
		if s.monitor == nil || s.monitor.IsHealthy(name) {
			healthy = append(healthy, name)
		}
	}

	if len(healthy) == 0 {
		return nil
	}

	if count >= len(healthy) {
		return healthy
	}

	var bCold []string
	var bHot []string
	for _, name := range healthy {
		b, ok := s.backends[name]
		if ok && s.hasAffinityTag(b) {
			bCold = append(bCold, name)
		} else {
			bHot = append(bHot, name)
		}
	}

	selectedMap := make(map[string]bool)
	var selected []string

	if len(bCold) > 0 {
		sort.Slice(bCold, func(i, j int) bool {
			spaceI := s.getFreeSpace(bCold[i])
			spaceJ := s.getFreeSpace(bCold[j])
			if spaceI != spaceJ {
				return spaceI > spaceJ
			}
			return bCold[i] < bCold[j]
		})
		topCold := bCold[0]
		selected = append(selected, topCold)
		selectedMap[topCold] = true
	}

	var remaining []string
	for _, name := range bHot {
		if !selectedMap[name] {
			remaining = append(remaining, name)
		}
	}
	for _, name := range bCold {
		if !selectedMap[name] {
			remaining = append(remaining, name)
		}
	}

	sort.Slice(remaining, func(i, j int) bool {
		spaceI := s.getFreeSpace(remaining[i])
		spaceJ := s.getFreeSpace(remaining[j])
		if spaceI != spaceJ {
			return spaceI > spaceJ
		}
		return remaining[i] < remaining[j]
	})

	needed := count - len(selected)
	for i := 0; i < needed && i < len(remaining); i++ {
		selected = append(selected, remaining[i])
	}

	return selected
}

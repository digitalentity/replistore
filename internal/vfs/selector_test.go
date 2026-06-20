package vfs_test

import (
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSmartSelector_SelectForRead(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1", SpeedVal: 10}
	b2 := &bmock.MockBackend{NameVal: "b2", SpeedVal: 5}
	b3 := &bmock.MockBackend{NameVal: "b3", SpeedVal: 10}

	backends := map[string]backend.Backend{
		"b1": b1,
		"b2": b2,
		"b3": b3,
	}

	monitor := backend.NewHealthMonitor(backends)
	selector := vfs.NewSmartSelector(backends, monitor, nil)

	meta := vfs.Metadata{
		Backends: []string{"b1", "b2", "b3"},
	}

	// Since b1 and b3 have speed 10, and b2 has speed 5, SelectForRead should only pick b1 or b3
	selectedCount := map[string]int{}
	for range 100 {
		b := selector.SelectForRead(meta)
		selectedCount[b]++
	}

	assert.Positive(t, selectedCount["b1"], "should select b1 sometimes")
	assert.Positive(t, selectedCount["b3"], "should select b3 sometimes")
	assert.Equal(t, 0, selectedCount["b2"], "should never select b2 because it is slower")

	// Mock Ping behavior to test health check integration
	b1.On("Ping", mock.Anything).Return(assert.AnError)
	b2.On("Ping", mock.Anything).Return(nil)
	b3.On("Ping", mock.Anything).Return(nil)

	ctx := t.Context()

	monitor.Start(ctx, 1*time.Millisecond)
	time.Sleep(10 * time.Millisecond) // Wait for health check to run

	// Since b1 is unhealthy, only b3 (speed 10) is the fastest healthy candidate
	for range 50 {
		b := selector.SelectForRead(meta)
		assert.Equal(t, "b3", b, "should select b3 since b1 is down and b2 is slower")
	}
}

func TestSmartSelector_SelectForWrite(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1", SpeedVal: 10, TagsVal: []string{"hot"}}
	b2 := &bmock.MockBackend{NameVal: "b2", SpeedVal: 5, TagsVal: []string{"hot"}}
	b3 := &bmock.MockBackend{NameVal: "b3", SpeedVal: 1, TagsVal: []string{"cold"}}
	b4 := &bmock.MockBackend{NameVal: "b4", SpeedVal: 1, TagsVal: []string{"cold"}}

	backends := map[string]backend.Backend{
		"b1": b1,
		"b2": b2,
		"b3": b3,
		"b4": b4,
	}

	// Mock GetFreeSpace calls
	b1.On("GetFreeSpace").Return(uint64(100), nil)
	b2.On("GetFreeSpace").Return(uint64(50), nil)
	b3.On("GetFreeSpace").Return(uint64(80), nil)
	b4.On("GetFreeSpace").Return(uint64(20), nil)

	// Selector with write affinity to "cold"
	selector := vfs.NewSmartSelector(backends, nil, []string{"cold"})

	allBackends := []string{"b1", "b2", "b3", "b4"}

	// Case 1: Count = 1. Should pick the cold backend with most free space (b3)
	res1 := selector.SelectForWrite(1, allBackends)
	assert.Equal(t, []string{"b3"}, res1)

	// Case 2: Count = 2. First pick is b3 (cold with most space). Second pick is b1 (remaining hot/cold with most space, which is b1 with 100)
	res2 := selector.SelectForWrite(2, allBackends)
	assert.Equal(t, []string{"b3", "b1"}, res2)

	// Case 3: Count = 3. Picks:
	// - b3 (cold with most space)
	// - remaining: b1 (100), b2 (50), b4 (20). Sorted: b1, b2, b4.
	// - Picks b1, b2.
	// Result: ["b3", "b1", "b2"]
	res3 := selector.SelectForWrite(3, allBackends)
	assert.Equal(t, []string{"b3", "b1", "b2"}, res3)

	// Case 4: Count = 4. Returns all healthy backends.
	res4 := selector.SelectForWrite(4, allBackends)
	assert.Len(t, res4, 4)
	assert.Contains(t, res4, "b1")
	assert.Contains(t, res4, "b2")
	assert.Contains(t, res4, "b3")
	assert.Contains(t, res4, "b4")

	// Case 5: Count = 5 (larger than healthy count). Returns all healthy backends.
	res5 := selector.SelectForWrite(5, allBackends)
	assert.Len(t, res5, 4)
}

func TestSmartSelector_SelectForWrite_NoAffinity(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1", SpeedVal: 10, TagsVal: []string{"hot"}}
	b2 := &bmock.MockBackend{NameVal: "b2", SpeedVal: 5, TagsVal: []string{"hot"}}
	b3 := &bmock.MockBackend{NameVal: "b3", SpeedVal: 1, TagsVal: []string{"cold"}}

	backends := map[string]backend.Backend{
		"b1": b1,
		"b2": b2,
		"b3": b3,
	}

	b1.On("GetFreeSpace").Return(uint64(100), nil)
	b2.On("GetFreeSpace").Return(uint64(150), nil)
	b3.On("GetFreeSpace").Return(uint64(80), nil)

	// No affinity tags configured
	selector := vfs.NewSmartSelector(backends, nil, nil)

	allBackends := []string{"b1", "b2", "b3"}

	// Should sort strictly by free space: b2 (150), b1 (100), b3 (80)
	res := selector.SelectForWrite(2, allBackends)
	assert.Equal(t, []string{"b2", "b1"}, res)
}

func TestSmartSelector_SelectForWrite_GetFreeSpaceCalledOnce(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1", SpeedVal: 10, TagsVal: []string{"hot"}}
	b2 := &bmock.MockBackend{NameVal: "b2", SpeedVal: 5, TagsVal: []string{"hot"}}

	backends := map[string]backend.Backend{
		"b1": b1,
		"b2": b2,
	}

	b1.On("GetFreeSpace").Return(uint64(100), nil).Once()
	b2.On("GetFreeSpace").Return(uint64(150), nil).Once()

	selector := vfs.NewSmartSelector(backends, nil, nil)
	allBackends := []string{"b1", "b2"}

	res := selector.SelectForWrite(1, allBackends)
	assert.Equal(t, []string{"b2"}, res)

	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}


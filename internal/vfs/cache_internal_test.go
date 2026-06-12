package vfs

// Tests for generation-aware reconciliation (Phase 1b). These live in package
// vfs (not vfs_test) because mergeMeta and syncAll are unexported.

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMergeMeta_FileGenRules(t *testing.T) {
	now := time.Now().Round(time.Second)
	older := now.Add(-time.Hour)
	newer := now.Add(time.Hour)

	file := func(gen int64, size int64, mtime time.Time, backends ...string) Metadata {
		return Metadata{Name: "f", Path: "f", Size: size, ModTime: mtime, Gen: gen, Backends: backends}
	}

	cases := []struct {
		name     string
		existing Metadata
		incoming Metadata
		want     Metadata // Backends compared as a set
	}{
		// Rule 1: higher Gen wins outright.
		{
			// REVIEW C4.2 data-loss scenario: a skewed clock gave the stale
			// gen-1 replica a newer mtime; gen 2 must still win.
			name:     "higher gen beats newer mtime",
			existing: file(1, 100, newer, "b1"),
			incoming: file(2, 50, older, "b2"),
			want:     file(2, 50, older, "b2"),
		},
		{
			name:     "lower gen ignored despite newer mtime and larger size",
			existing: file(2, 50, older, "b1"),
			incoming: file(1, 100, newer, "b2"),
			want:     file(2, 50, older, "b1"),
		},
		// Rule 2: equal Gen, equal Size → union; newest mtime for display.
		{
			name:     "equal gen equal size divergent mtimes union (churn killer)",
			existing: file(3, 100, now, "b1"),
			incoming: file(3, 100, newer, "b2"),
			want:     file(3, 100, newer, "b1", "b2"),
		},
		{
			name:     "equal gen equal size older incoming mtime still unions",
			existing: file(3, 100, now, "b1"),
			incoming: file(3, 100, older, "b2"),
			want:     file(3, 100, now, "b1", "b2"),
		},
		{
			name:     "both gen0 same size different mtime union (legacy C4 mitigation)",
			existing: file(0, 100, now, "b1"),
			incoming: file(0, 100, newer, "b2"),
			want:     file(0, 100, newer, "b1", "b2"),
		},
		// Rule 3: equal Gen, different Size → (mtime, size) LWW.
		{
			name:     "equal gen different size newer mtime wins",
			existing: file(3, 100, now, "b1"),
			incoming: file(3, 50, newer, "b2"),
			want:     file(3, 50, newer, "b2"),
		},
		{
			name:     "equal gen different size stale mtime ignored",
			existing: file(3, 100, now, "b1"),
			incoming: file(3, 50, older, "b2"),
			want:     file(3, 100, now, "b1"),
		},
		{
			name:     "equal gen different size same mtime larger wins",
			existing: file(3, 100, now, "b1"),
			incoming: file(3, 150, now, "b2"),
			want:     file(3, 150, now, "b2"),
		},
		{
			name:     "both gen0 newer mtime different size wins (legacy LWW)",
			existing: file(0, 100, now, "b1"),
			incoming: file(0, 50, newer, "b2"),
			want:     file(0, 50, newer, "b2"),
		},
		// Mixed knowledge: exactly one side gen 0 → legacy (mtime, size)
		// rules, but the stored Gen never goes down (keep max of the two).
		{
			name:     "gen0 incoming same version unions and keeps gen",
			existing: file(5, 100, now, "b1"),
			incoming: file(0, 100, now, "b2"),
			want:     file(5, 100, now, "b1", "b2"),
		},
		{
			name:     "gen0 incoming newer mtime replaces backends but keeps gen",
			existing: file(5, 100, now, "b1"),
			incoming: file(0, 100, newer, "b2"),
			want:     file(5, 100, newer, "b2"),
		},
		{
			name:     "gen0 incoming stale mtime ignored and gen kept",
			existing: file(5, 100, now, "b1"),
			incoming: file(0, 200, older, "b2"),
			want:     file(5, 100, now, "b1"),
		},
		{
			name:     "gen5 incoming vs gen0 existing follows legacy rules, gen upgraded",
			existing: file(0, 100, now, "b1"),
			incoming: file(5, 50, newer, "b2"),
			want:     file(5, 50, newer, "b2"),
		},
		{
			name:     "gen5 incoming stale mtime vs gen0 existing keeps data, upgrades gen",
			existing: file(0, 100, newer, "b1"),
			incoming: file(5, 50, now, "b2"),
			want:     file(5, 100, newer, "b1"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.existing
			got.Backends = append([]string(nil), tc.existing.Backends...)
			mergeMeta(&got, tc.incoming, tc.incoming.Backends)

			assert.Equal(t, tc.want.Size, got.Size, "size")
			assert.Equal(t, tc.want.ModTime, got.ModTime, "mtime")
			assert.Equal(t, tc.want.Gen, got.Gen, "gen")
			assert.ElementsMatch(t, tc.want.Backends, got.Backends, "backends")
			assert.False(t, got.IsDir)
		})
	}
}

func TestMergeMeta_DirRulesUnchanged(t *testing.T) {
	now := time.Now().Round(time.Second)

	t.Run("dirs union regardless of mtime and gen", func(t *testing.T) {
		existing := Metadata{Name: "d", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now, Backends: []string{"b1"}}
		incoming := Metadata{Name: "d", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now.Add(-time.Hour), Gen: 7}
		mergeMeta(&existing, incoming, []string{"b2"})
		assert.True(t, existing.IsDir)
		assert.ElementsMatch(t, []string{"b1", "b2"}, existing.Backends)
		assert.Equal(t, now, existing.ModTime)
	})

	t.Run("dir wins type conflict over higher-gen file", func(t *testing.T) {
		existing := Metadata{Name: "x", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now, Backends: []string{"b1"}}
		incoming := Metadata{Name: "x", Size: 10, ModTime: now.Add(time.Hour), Gen: 9}
		mergeMeta(&existing, incoming, []string{"b2"})
		assert.True(t, existing.IsDir)
		assert.ElementsMatch(t, []string{"b1", "b2"}, existing.Backends)
	})
}

// mockWalk makes b's Walk report exactly the given path/info pairs.
func mockWalk(b *test.MockBackend, entries map[string]backend.FileInfo) {
	b.On("Walk", mock.Anything, "", mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(string, backend.FileInfo) error)
		for path, info := range entries {
			_ = fn(path, info)
		}
	}).Return(nil)
}

// mockSidecarRead makes b serve a valid sidecar with the given generation for
// path.
func mockSidecarRead(b *test.MockBackend, path string, gen int64) {
	payload := []byte(fmt.Sprintf(`{"v":1,"gen":%d,"writer":"w","deleted":false}`, gen))
	f := &test.MockFile{}
	f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), payload)
	}).Return(len(payload), io.EOF)
	f.On("Close").Return(nil)
	b.On("OpenFile", mock.Anything, SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(f, nil)
}

func TestSyncAll_SizeConflictResolvedByGenerations(t *testing.T) {
	ctx := context.Background()
	c := NewCache()
	now := time.Now().Round(time.Second)

	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	// Sizes differ → conflict → sidecars decide. b1 has a NEWER mtime but an
	// older generation: it must lose.
	mockWalk(b1, map[string]backend.FileInfo{
		"f.txt": {Name: "f.txt", Size: 100, ModTime: now.Add(time.Hour)},
	})
	mockWalk(b2, map[string]backend.FileInfo{
		"f.txt": {Name: "f.txt", Size: 200, ModTime: now},
	})
	mockSidecarRead(b1, "f.txt", 3)
	mockSidecarRead(b2, "f.txt", 5)

	c.syncAll(ctx, []backend.Backend{b1, b2})

	node, ok := c.Get("f.txt")
	assert.True(t, ok)
	assert.Equal(t, []string{"b2"}, node.Meta.Backends)
	assert.Equal(t, int64(200), node.Meta.Size)
	assert.Equal(t, now, node.Meta.ModTime)
	assert.Equal(t, int64(5), node.Meta.Gen)
	b1.AssertExpectations(t)
	b2.AssertExpectations(t)
}

func TestSyncAll_SameSizeMergesWithoutSidecarReads(t *testing.T) {
	ctx := context.Background()
	c := NewCache()
	now := time.Now().Round(time.Second)

	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	// Equal sizes with divergent server-stamped mtimes: one version, both
	// backends, no sidecar reads.
	mockWalk(b1, map[string]backend.FileInfo{
		"f.txt": {Name: "f.txt", Size: 100, ModTime: now},
	})
	mockWalk(b2, map[string]backend.FileInfo{
		"f.txt": {Name: "f.txt", Size: 100, ModTime: now.Add(time.Hour)},
	})

	c.syncAll(ctx, []backend.Backend{b1, b2})

	node, ok := c.Get("f.txt")
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	assert.Equal(t, int64(100), node.Meta.Size)
	assert.Equal(t, now.Add(time.Hour), node.Meta.ModTime)
	assert.Equal(t, int64(0), node.Meta.Gen)
	b1.AssertNotCalled(t, "OpenFile", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	b2.AssertNotCalled(t, "OpenFile", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestSyncAll_FileDirConflictSkipsSidecars(t *testing.T) {
	ctx := context.Background()
	c := NewCache()
	now := time.Now().Round(time.Second)

	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}

	// File on b1, directory on b2: dir wins on presence, no sidecar reads.
	mockWalk(b1, map[string]backend.FileInfo{
		"x": {Name: "x", Size: 100, ModTime: now},
	})
	mockWalk(b2, map[string]backend.FileInfo{
		"x": {Name: "x", IsDir: true, Mode: os.ModeDir | 0755, ModTime: now},
	})

	c.syncAll(ctx, []backend.Backend{b1, b2})

	node, ok := c.Get("x")
	assert.True(t, ok)
	assert.True(t, node.Meta.IsDir)
	assert.ElementsMatch(t, []string{"b1", "b2"}, node.Meta.Backends)
	b1.AssertNotCalled(t, "OpenFile", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	b2.AssertNotCalled(t, "OpenFile", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestFetchEntry_PopulatesGenFromSidecar(t *testing.T) {
	ctx := context.Background()
	c := NewCache()
	now := time.Now().Round(time.Second)

	b1 := &test.MockBackend{NameVal: "b1"}
	b2 := &test.MockBackend{NameVal: "b2"}
	path := "dir/f.txt"

	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{Name: "f.txt", Size: 100, ModTime: now}, nil)
	b2.On("Stat", mock.Anything, path).Return(backend.FileInfo{Name: "f.txt", Size: 200, ModTime: now.Add(time.Hour)}, nil)
	mockSidecarRead(b1, path, 7) // higher gen on the smaller/older replica
	mockSidecarRead(b2, path, 4)

	node, err := c.FetchEntry(ctx, path, []backend.Backend{b1, b2})
	assert.NoError(t, err)
	assert.Equal(t, int64(7), node.Meta.Gen)
	assert.Equal(t, int64(100), node.Meta.Size)
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)
}

func TestFetchEntry_MissingSidecarMeansGenZero(t *testing.T) {
	ctx := context.Background()
	c := NewCache()
	now := time.Now().Round(time.Second)

	b1 := &test.MockBackend{NameVal: "b1"}
	path := "f.txt"

	b1.On("Stat", mock.Anything, path).Return(backend.FileInfo{Name: "f.txt", Size: 100, ModTime: now}, nil)
	b1.On("OpenFile", mock.Anything, SidecarPath(path), os.O_RDONLY, os.FileMode(0)).Return(nil, os.ErrNotExist)

	node, err := c.FetchEntry(ctx, path, []backend.Backend{b1})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), node.Meta.Gen)
	assert.Equal(t, []string{"b1"}, node.Meta.Backends)
}

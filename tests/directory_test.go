package tests

import (
	"sort"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSMB_MkdirAllNestedAndReadDir(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	require.NoError(t, b.MkdirAll(ctx, "a/b/c", 0755))

	for _, dir := range []string{"a", "a/b", "a/b/c"} {
		info, err := b.Stat(ctx, dir)
		require.NoError(t, err, "stat %s", dir)
		assert.True(t, info.IsDir, "%s must be a directory", dir)
	}

	entries, err := b.ReadDir(ctx, "a")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "b", entries[0].Name)
	assert.True(t, entries[0].IsDir)
}

func TestSMB_WalkVisitsWholeTree(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	require.NoError(t, b.MkdirAll(ctx, "dir1/sub", 0755))
	writeBackendFile(t, ctx, b, "root.txt", []byte("r"))
	writeBackendFile(t, ctx, b, "dir1/f1.txt", []byte("a"))
	writeBackendFile(t, ctx, b, "dir1/sub/f2.txt", []byte("b"))

	var visited []string
	err := b.Walk(ctx, "", func(path string, _ backend.FileInfo) error {
		visited = append(visited, path)

		return nil
	})
	require.NoError(t, err)

	sort.Strings(visited)
	assert.Equal(t, []string{
		"dir1",
		"dir1/f1.txt",
		"dir1/sub",
		"dir1/sub/f2.txt",
		"root.txt",
	}, visited)
}

func TestSMB_RenameFile(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	content := []byte("rename me")
	writeBackendFile(t, ctx, b, "old.txt", content)

	require.NoError(t, b.Rename(ctx, "old.txt", "new.txt"))

	_, err := b.Stat(ctx, "old.txt")
	require.Error(t, err, "old path must be gone")

	info, err := b.Stat(ctx, "new.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), info.Size)
	assert.Equal(t, content, readBackendFile(t, ctx, b, "new.txt"))
}

func TestSMB_RenameDirectory(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	require.NoError(t, b.MkdirAll(ctx, "src", 0755))
	writeBackendFile(t, ctx, b, "src/f.txt", []byte("moved"))

	require.NoError(t, b.Rename(ctx, "src", "dst"))

	_, err := b.Stat(ctx, "src")
	require.Error(t, err)

	assert.Equal(t, []byte("moved"), readBackendFile(t, ctx, b, "dst/f.txt"))
}

func TestSMB_RemoveFileAndDir(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	require.NoError(t, b.MkdirAll(ctx, "gone", 0755))
	writeBackendFile(t, ctx, b, "gone.txt", []byte("x"))

	require.NoError(t, b.Remove(ctx, "gone.txt"))
	_, err := b.Stat(ctx, "gone.txt")
	require.Error(t, err)

	require.NoError(t, b.Remove(ctx, "gone"))
	_, err = b.Stat(ctx, "gone")
	require.Error(t, err)
}

// TestSMB_ChtimesAccepted exercises the Chtimes request path end to end. The
// in-memory test server accepts the SET_INFO request but does not persist the
// timestamp, so this asserts the call succeeds rather than that Stat reflects
// the new mtime; mtime persistence is covered against a real server elsewhere.
func TestSMB_ChtimesAccepted(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	writeBackendFile(t, ctx, b, "stamp.txt", []byte("t"))

	want := time.Date(2021, time.March, 4, 5, 6, 7, 0, time.UTC)
	require.NoError(t, b.Chtimes(ctx, "stamp.txt", want, want))

	_, err := b.Stat(ctx, "stamp.txt")
	require.NoError(t, err)
}

func TestSMB_FreeAndTotalSpace(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	// Stat the share so a connection is established before querying space.
	require.NoError(t, b.Ping(ctx))

	free, err := b.GetFreeSpace()
	require.NoError(t, err)
	total, err := b.GetTotalSpace()
	require.NoError(t, err)
	assert.LessOrEqual(t, free, total, "free space cannot exceed total")
}

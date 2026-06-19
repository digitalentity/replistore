package tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readBackendFile reads name in full via repeated ReadAt, tolerating short
// reads, and returns the bytes.
func readBackendFile(t *testing.T, ctx context.Context, b backend.Backend, name string) []byte {
	t.Helper()
	info, err := b.Stat(ctx, name)
	require.NoError(t, err)

	f, err := b.OpenFile(ctx, name, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, info.Size)
	var off int64
	for off < info.Size {
		n, err := f.ReadAt(ctx, buf[off:], off)
		off += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
	}

	return buf[:off]
}

// patternBytes returns n deterministic bytes for content round-trip checks.
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i * 7) % 251)
	}

	return b
}

func TestSMB_LargeFileRoundTrip(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	content := patternBytes(512 * 1024) // 512 KiB, well past a single tiny write.
	writeBackendFile(t, ctx, b, "large.bin", content)

	got := readBackendFile(t, ctx, b, "large.bin")
	require.Len(t, got, len(content))
	assert.True(t, bytes.Equal(content, got), "round-tripped content must match")

	// A read at an interior offset must return the matching slice.
	f, err := b.OpenFile(ctx, "large.bin", os.O_RDONLY, 0)
	require.NoError(t, err)
	defer f.Close()

	const off = 300 * 1024
	mid := make([]byte, 4096)
	n, err := f.ReadAt(ctx, mid, off)
	require.NoError(t, err)
	assert.Equal(t, content[off:off+int64(n)], mid[:n])
}

func TestSMB_OffsetWriteLeavesHole(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	f, err := b.OpenFile(ctx, "sparse.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)

	head := []byte("AAAA")
	tail := []byte("BBBB")
	_, err = f.WriteAt(ctx, head, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(ctx, tail, 10)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	got := readBackendFile(t, ctx, b, "sparse.bin")
	require.Len(t, got, 14)
	assert.Equal(t, head, got[0:4], "head bytes")
	assert.Equal(t, make([]byte, 6), got[4:10], "hole must read back as zeros")
	assert.Equal(t, tail, got[10:14], "tail bytes")
}

func TestSMB_SequentialOffsetWrites(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	f, err := b.OpenFile(ctx, "appended.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)

	chunks := [][]byte{[]byte("hello "), []byte("from "), []byte("replistore")}
	var off int64
	var want []byte
	for _, c := range chunks {
		n, err := f.WriteAt(ctx, c, off)
		require.NoError(t, err)
		require.Equal(t, len(c), n)
		off += int64(n)
		want = append(want, c...)
	}
	require.NoError(t, f.Close())

	got := readBackendFile(t, ctx, b, "appended.bin")
	assert.Equal(t, want, got)
}

func TestSMB_ZeroByteFile(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	f, err := b.OpenFile(ctx, "empty.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	info, err := b.Stat(ctx, "empty.bin")
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size)
	assert.False(t, info.IsDir)

	got := readBackendFile(t, ctx, b, "empty.bin")
	assert.Empty(t, got)
}

func TestSMB_TruncateGrowAndShrink(t *testing.T) {
	s := startTestSMBServer(t, "share")
	ctx := t.Context()
	b := connectBackendVia(t, ctx, addrOf(s))

	writeBackendFile(t, ctx, b, "trunc.bin", []byte("hello world"))

	// Shrink.
	require.NoError(t, b.Truncate(ctx, "trunc.bin", 5))
	info, err := b.Stat(ctx, "trunc.bin")
	require.NoError(t, err)
	require.Equal(t, int64(5), info.Size)
	assert.Equal(t, []byte("hello"), readBackendFile(t, ctx, b, "trunc.bin"))

	// Grow: the extension must read back as zeros.
	require.NoError(t, b.Truncate(ctx, "trunc.bin", 8))
	info, err = b.Stat(ctx, "trunc.bin")
	require.NoError(t, err)
	require.Equal(t, int64(8), info.Size)
	assert.Equal(t, []byte("hello\x00\x00\x00"), readBackendFile(t, ctx, b, "trunc.bin"))
}

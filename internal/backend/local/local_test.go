package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalBackend_Lifecycle(t *testing.T) {
	// Create a temp directory for the local backend
	tmpDir := t.TempDir()

	// 1. Create the backend via factory Create
	opts := map[string]any{
		"path": tmpDir,
	}
	b, err := backend.Create("local", "test-local", opts)
	require.NoError(t, err, "failed to create local backend")
	assert.Equal(t, "test-local", b.GetName())

	// 2. Connect
	err = b.Connect(context.Background())
	require.NoError(t, err, "connect failed")

	// 3. Ping
	ctx := context.Background()
	err = b.Ping(ctx)
	require.NoError(t, err, "ping failed")

	// 4. Mkdir & OpenFile (Write/Read)
	dirPath := "testdir"
	err = b.Mkdir(ctx, dirPath, 0755)
	require.NoError(t, err, "mkdir failed")

	filePath := filepath.Join(dirPath, "hello.txt")
	f, err := b.OpenFile(ctx, filePath, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err, "openfile failed")

	content := []byte("hello replistore local backend")
	n, err := f.WriteAt(ctx, content, 0)
	require.NoError(t, err, "write failed")
	assert.Equal(t, len(content), n, "expected to write %d bytes, wrote %d", len(content), n)

	err = f.Sync(ctx)
	require.NoError(t, err, "sync failed")

	err = f.Close()
	require.NoError(t, err, "close failed")

	// 5. Stat
	fi, err := b.Stat(ctx, filePath)
	require.NoError(t, err, "stat failed")
	assert.Equal(t, "hello.txt", fi.Name)
	assert.Equal(t, int64(len(content)), fi.Size)
	assert.False(t, fi.IsDir, "expected file, got directory")

	// 6. ReadDir
	entries, err := b.ReadDir(ctx, dirPath)
	require.NoError(t, err, "readdir failed")
	require.Len(t, entries, 1, "expected 1 entry in directory, got %d", len(entries))
	assert.Equal(t, "hello.txt", entries[0].Name)

	// 7. Walk
	walked := map[string]bool{}
	err = b.Walk(ctx, ".", func(path string, info backend.FileInfo) error {
		walked[path] = info.IsDir

		return nil
	})
	require.NoError(t, err, "walk failed")

	expectedWalk := map[string]bool{
		"testdir":           true,
		"testdir/hello.txt": false,
	}

	for k, v := range expectedWalk {
		gotIsDir, exists := walked[k]
		if assert.Truef(t, exists, "expected path %q in walk, but it was not walked", k) {
			assert.Equalf(t, v, gotIsDir, "expected path %q isDir to be %v, got %v", k, v, gotIsDir)
		}
	}

	// 8. Chtimes & Truncate
	newTime := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
	err = b.Chtimes(ctx, filePath, newTime, newTime)
	require.NoError(t, err, "chtimes failed")

	statFi, err := b.Stat(ctx, filePath)
	require.NoError(t, err, "stat failed")
	assert.WithinDuration(t, newTime, statFi.ModTime, time.Second)

	err = b.Truncate(ctx, filePath, 5)
	require.NoError(t, err, "truncate failed")

	statFi, err = b.Stat(ctx, filePath)
	require.NoError(t, err, "stat failed")
	assert.Equal(t, int64(5), statFi.Size)

	// Read truncated file to verify
	rf, err := b.OpenFile(ctx, filePath, os.O_RDONLY, 0)
	require.NoError(t, err, "open truncated file failed")
	defer rf.Close()

	readBuf := make([]byte, 10)
	rn, rerr := rf.ReadAt(ctx, readBuf, 0)
	if rerr != nil {
		require.ErrorIs(t, rerr, io.EOF)
	}
	assert.Equal(t, 5, rn)
	assert.Equal(t, "hello", string(readBuf[:rn]))

	// 9. Rename & Remove
	newFilePath := filepath.Join(dirPath, "hello_renamed.txt")
	err = b.Rename(ctx, filePath, newFilePath)
	require.NoError(t, err, "rename failed")

	// Old file should not exist
	_, err = b.Stat(ctx, filePath)
	assert.True(t, os.IsNotExist(err), "expected old file to not exist, got err: %v", err)

	// New file should exist
	_, err = b.Stat(ctx, newFilePath)
	require.NoError(t, err, "renamed file access error")

	// Remove renamed file
	err = b.Remove(ctx, newFilePath)
	require.NoError(t, err, "remove failed")

	_, err = b.Stat(ctx, newFilePath)
	assert.True(t, os.IsNotExist(err), "expected removed file to not exist, got err: %v", err)
}

func TestLocalBackend_DirectoryTraversalPrevention(t *testing.T) {
	tmpDir := t.TempDir()
	b := NewLocalBackend("test-local", tmpDir, 10, nil)

	// Try to resolve paths that attempt directory traversal escape
	resolved := b.resolve("../../../etc/passwd")
	expected := filepath.Join(tmpDir, "etc/passwd")
	assert.Equal(t, expected, resolved)

	resolvedDot := b.resolve(".")
	assert.Equal(t, tmpDir, resolvedDot)

	resolvedSlash := b.resolve("/")
	assert.Equal(t, tmpDir, resolvedSlash)
}

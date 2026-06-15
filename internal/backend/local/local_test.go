package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
)

func TestLocalBackend_Lifecycle(t *testing.T) {
	// Create a temp directory for the local backend
	tmpDir := t.TempDir()

	// 1. Create the backend via factory Create
	opts := map[string]interface{}{
		"path": tmpDir,
	}
	b, err := backend.Create("local", "test-local", opts)
	if err != nil {
		t.Fatalf("failed to create local backend: %v", err)
	}

	if b.GetName() != "test-local" {
		t.Errorf("expected backend name 'test-local', got %q", b.GetName())
	}

	// 2. Connect
	if err := b.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// 3. Ping
	ctx := context.Background()
	if err := b.Ping(ctx); err != nil {
		t.Errorf("ping failed: %v", err)
	}

	// 4. Mkdir & OpenFile (Write/Read)
	dirPath := "testdir"
	if err := b.Mkdir(ctx, dirPath, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	filePath := filepath.Join(dirPath, "hello.txt")
	f, err := b.OpenFile(ctx, filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("openfile failed: %v", err)
	}

	content := []byte("hello replistore local backend")
	n, err := f.WriteAt(ctx, content, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len(content) {
		t.Errorf("expected to write %d bytes, wrote %d", len(content), n)
	}

	if err := f.Sync(ctx); err != nil {
		t.Errorf("sync failed: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}

	// 5. Stat
	fi, err := b.Stat(ctx, filePath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if fi.Name != "hello.txt" {
		t.Errorf("expected file name 'hello.txt', got %q", fi.Name)
	}
	if fi.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), fi.Size)
	}
	if fi.IsDir {
		t.Error("expected file, got directory")
	}

	// 6. ReadDir
	entries, err := b.ReadDir(ctx, dirPath)
	if err != nil {
		t.Fatalf("readdir failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry in directory, got %d", len(entries))
	}
	if entries[0].Name != "hello.txt" {
		t.Errorf("expected entry name 'hello.txt', got %q", entries[0].Name)
	}

	// 7. Walk
	walked := map[string]bool{}
	err = b.Walk(ctx, ".", func(path string, info backend.FileInfo) error {
		walked[path] = info.IsDir
		return nil
	})
	if err != nil {
		t.Fatalf("walk failed: %v", err)
	}

	expectedWalk := map[string]bool{
		"testdir":           true,
		"testdir/hello.txt": false,
	}

	for k, v := range expectedWalk {
		gotIsDir, exists := walked[k]
		if !exists {
			t.Errorf("expected path %q in walk, but it was not walked", k)
		} else if gotIsDir != v {
			t.Errorf("expected path %q isDir to be %v, got %v", k, v, gotIsDir)
		}
	}

	// 8. Chtimes & Truncate
	newTime := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
	if err := b.Chtimes(ctx, filePath, newTime, newTime); err != nil {
		t.Errorf("chtimes failed: %v", err)
	}

	statFi, err := b.Stat(ctx, filePath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	// Check ModTime difference within a reasonable margin due to filesystem resolution
	if diff := statFi.ModTime.Sub(newTime); diff > time.Second || diff < -time.Second {
		t.Errorf("expected mod time close to %v, got %v (diff %v)", newTime, statFi.ModTime, diff)
	}

	if err := b.Truncate(ctx, filePath, 5); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	statFi, err = b.Stat(ctx, filePath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if statFi.Size != 5 {
		t.Errorf("expected size 5 after truncate, got %d", statFi.Size)
	}

	// Read truncated file to verify
	rf, err := b.OpenFile(ctx, filePath, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open truncated file failed: %v", err)
	}
	defer rf.Close()

	readBuf := make([]byte, 10)
	rn, rerr := rf.ReadAt(ctx, readBuf, 0)
	if rerr != nil && rerr != io.EOF {
		t.Fatalf("read truncated file failed: %v", rerr)
	}
	if rn != 5 {
		t.Errorf("expected to read 5 bytes, got %d", rn)
	}
	if string(readBuf[:rn]) != "hello" {
		t.Errorf("expected content 'hello', got %q", string(readBuf[:rn]))
	}

	// 9. Rename & Remove
	newFilePath := filepath.Join(dirPath, "hello_renamed.txt")
	if err := b.Rename(ctx, filePath, newFilePath); err != nil {
		t.Fatalf("rename failed: %v", err)
	}

	// Old file should not exist
	_, err = b.Stat(ctx, filePath)
	if !os.IsNotExist(err) {
		t.Errorf("expected old file to not exist, got err: %v", err)
	}

	// New file should exist
	_, err = b.Stat(ctx, newFilePath)
	if err != nil {
		t.Errorf("renamed file access error: %v", err)
	}

	// Remove renamed file
	if err := b.Remove(ctx, newFilePath); err != nil {
		t.Fatalf("remove failed: %v", err)
	}

	_, err = b.Stat(ctx, newFilePath)
	if !os.IsNotExist(err) {
		t.Errorf("expected removed file to not exist, got err: %v", err)
	}
}

func TestLocalBackend_DirectoryTraversalPrevention(t *testing.T) {
	tmpDir := t.TempDir()
	b := NewLocalBackend("test-local", tmpDir, 10, nil)

	// Try to resolve paths that attempt directory traversal escape
	resolved := b.resolve("../../../etc/passwd")
	expected := filepath.Join(tmpDir, "etc/passwd")
	if resolved != expected {
		t.Errorf("expected traversal prevention to resolve to %q, got %q", expected, resolved)
	}

	resolvedDot := b.resolve(".")
	if resolvedDot != tmpDir {
		t.Errorf("expected resolving '.' to be %q, got %q", tmpDir, resolvedDot)
	}

	resolvedSlash := b.resolve("/")
	if resolvedSlash != tmpDir {
		t.Errorf("expected resolving '/' to be %q, got %q", tmpDir, resolvedSlash)
	}
}

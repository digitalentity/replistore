package tests

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/absfs/memfs"
	"github.com/absfs/smbfs"
	"github.com/digitalentity/replistore/internal/backend"
	"github.com/digitalentity/replistore/internal/backend/smb"
	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestSMBServer struct {
	Server  *smbfs.Server
	MemFS   *memfs.FileSystem
	Port    int
	Share   string
	stopped bool
	mu      sync.Mutex
}

func findFreePort() (int, error) {
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func startTestSMBServer(t *testing.T, shareName string) *TestSMBServer {
	t.Helper()
	mfs, err := memfs.NewFS()
	require.NoError(t, err)

	port, err := findFreePort()
	require.NoError(t, err)

	serverOpts := smbfs.ServerOptions{
		Port:            port,
		Hostname:        "127.0.0.1",
		AllowGuest:      true,
		SigningRequired: false,
		Users: map[string]string{
			"testuser": "testpass",
		},
	}
	server, err := smbfs.NewServer(serverOpts)
	require.NoError(t, err)

	shareOpts := smbfs.ShareOptions{
		ShareName:  shareName,
		SharePath:  "/",
		AllowGuest: true,
	}
	err = server.AddShare(mfs, shareOpts)
	require.NoError(t, err)

	err = server.Listen()
	require.NoError(t, err)

	return &TestSMBServer{
		Server: server,
		MemFS:  mfs,
		Port:   port,
		Share:  shareName,
	}
}

func (s *TestSMBServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	if s.Server != nil {
		_ = s.Server.Stop()
	}
}

func (s *TestSMBServer) Restart() error {
	s.Stop()

	s.mu.Lock()
	s.stopped = false
	s.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	serverOpts := smbfs.ServerOptions{
		Port:            s.Port,
		Hostname:        "127.0.0.1",
		AllowGuest:      true,
		SigningRequired: false,
		Users: map[string]string{
			"testuser": "testpass",
		},
	}
	server, err := smbfs.NewServer(serverOpts)
	if err != nil {
		return err
	}

	shareOpts := smbfs.ShareOptions{
		ShareName:  s.Share,
		SharePath:  "/",
		AllowGuest: true,
	}
	err = server.AddShare(s.MemFS, shareOpts)
	if err != nil {
		return err
	}

	err = server.Listen()
	if err != nil {
		return err
	}

	s.Server = server

	return nil
}

func setupRepliFS(t *testing.T, ctx context.Context, cancel context.CancelFunc, s1, s2, s3 *TestSMBServer, writeQuorum int) (*rfuse.RepliFS, *backend.HealthMonitor, func()) {
	t.Helper()
	addr1 := fmt.Sprintf("127.0.0.1:%d", s1.Port)
	addr2 := fmt.Sprintf("127.0.0.1:%d", s2.Port)
	addr3 := fmt.Sprintf("127.0.0.1:%d", s3.Port)

	b1 := smb.NewSMBBackend("b1", addr1, s1.Share, "testuser", "testpass", "", 10, nil)
	b2 := smb.NewSMBBackend("b2", addr2, s2.Share, "testuser", "testpass", "", 10, nil)
	b3 := smb.NewSMBBackend("b3", addr3, s3.Share, "testuser", "testpass", "", 10, nil)

	err := b1.Connect(ctx)
	require.NoError(t, err)
	err = b2.Connect(ctx)
	require.NoError(t, err)
	err = b3.Connect(ctx)
	require.NoError(t, err)

	backends := map[string]backend.Backend{
		"b1": b1,
		"b2": b2,
		"b3": b3,
	}

	monitor := backend.NewHealthMonitor(backends)
	monitor.Start(ctx, 20*time.Millisecond)

	cache := vfs.NewCache()
	replFS := &rfuse.RepliFS{
		Cache:             cache,
		Backends:          backends,
		ReplicationFactor: 3,
		WriteQuorum:       writeQuorum,
		Selector:          vfs.NewFirstSelector(monitor),
		NodeID:            "node-test",
		MaxIODuration:     2 * time.Second,
		CacheTTL:          5 * time.Minute,
	}

	cleanup := func() {
		cancel()
		time.Sleep(50 * time.Millisecond) // Let background loops realize context is canceled and exit
		_ = b1.Close()
		_ = b2.Close()
		_ = b3.Close()
	}

	return replFS, monitor, cleanup
}

func writeFile(ctx context.Context, dir *rfuse.Dir, name string, content []byte) error {
	req := &fuse.CreateRequest{Name: name, Mode: 0644, Flags: fuse.OpenReadWrite}
	resp := &fuse.CreateResponse{}
	_, h, err := dir.Create(ctx, req, resp)
	if err != nil {
		return err
	}
	fileHandle := h.(*rfuse.FileHandle)
	defer fileHandle.Release(ctx, &fuse.ReleaseRequest{})

	writeReq := &fuse.WriteRequest{Data: content, Offset: 0}
	writeResp := &fuse.WriteResponse{}
	err = fileHandle.Write(ctx, writeReq, writeResp)
	if err != nil {
		return err
	}
	if writeResp.Size != len(content) {
		return fmt.Errorf("short write: wrote %d of %d bytes", writeResp.Size, len(content))
	}

	return fileHandle.Flush(ctx, &fuse.FlushRequest{})
}

func readFile(ctx context.Context, file *rfuse.File) ([]byte, error) {
	h, err := file.Open(ctx, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	if err != nil {
		return nil, err
	}
	fileHandle := h.(*rfuse.FileHandle)
	defer fileHandle.Release(ctx, &fuse.ReleaseRequest{})

	var attr fuse.Attr
	err = file.Attr(ctx, &attr)
	if err != nil {
		return nil, err
	}

	//nolint:gosec // test file sizes comfortably fit an int.
	readReq := &fuse.ReadRequest{Size: int(attr.Size), Offset: 0}
	readResp := &fuse.ReadResponse{}
	err = fileHandle.Read(ctx, readReq, readResp)
	if err != nil {
		return nil, err
	}

	return readResp.Data, nil
}

func TestIntegration_BasicReplication(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	defer s1.Stop()
	s2 := startTestSMBServer(t, "share2")
	defer s2.Stop()
	s3 := startTestSMBServer(t, "share3")
	defer s3.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replFS, _, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 3)
	defer cleanup()

	rootNode, err := replFS.Root()
	require.NoError(t, err)
	dir := rootNode.(*rfuse.Dir)

	content := []byte("hello from integration tests")

	// 1. Write file
	err = writeFile(ctx, dir, "test.txt", content)
	require.NoError(t, err)

	// 2. Lookup & verify VFS layer
	node, err := dir.Lookup(ctx, "test.txt")
	require.NoError(t, err)
	file := node.(*rfuse.File)

	cachedNode, ok := replFS.Cache.Get("test.txt")
	require.True(t, ok)
	cachedNode.Mu.RLock()
	assert.Equal(t, int64(len(content)), cachedNode.Meta.Size)
	assert.ElementsMatch(t, []string{"b1", "b2", "b3"}, cachedNode.Meta.Backends)
	cachedNode.Mu.RUnlock()

	// 3. Read back from RepliFS
	data, err := readFile(ctx, file)
	require.NoError(t, err)
	assert.Equal(t, content, data)

	// 4. Verify underlying memfs filesystems of all three servers
	for i, s := range []*TestSMBServer{s1, s2, s3} {
		f, err := s.MemFS.Open("/test.txt")
		require.NoError(t, err, "file not found on server %d", i+1)
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		_ = f.Close()
		assert.Equal(t, content, b, "mismatched content on server %d", i+1)

		// Verify that sidecars exist
		sidecar, err := vfs.ReadMeta(ctx, replFS.Backends[fmt.Sprintf("b%d", i+1)], "test.txt")
		require.NoError(t, err)
		assert.False(t, sidecar.Deleted)
		assert.Equal(t, int64(1), sidecar.Gen)
	}
}

func TestIntegration_ReadFailover(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	defer s1.Stop()
	s2 := startTestSMBServer(t, "share2")
	defer s2.Stop()
	s3 := startTestSMBServer(t, "share3")
	defer s3.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 3)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	content := []byte("read failover data")
	err := writeFile(ctx, dir, "failover.txt", content)
	require.NoError(t, err)

	// Stop s3
	s3.Stop()
	_ = replFS.Backends["b3"].Close()

	// Wait for monitor to mark b3 down
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 1*time.Second, 20*time.Millisecond)

	// Reading should still succeed by routing to b1 or b2
	node, err := dir.Lookup(ctx, "failover.txt")
	require.NoError(t, err)
	file := node.(*rfuse.File)

	data, err := readFile(ctx, file)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestIntegration_WriteQuorumEnforcement(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	defer s1.Stop()
	s2 := startTestSMBServer(t, "share2")
	defer s2.Stop()
	s3 := startTestSMBServer(t, "share3")
	defer s3.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write Quorum = 2, so we can write even if 1 server is down
	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 2)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// Stop s3
	s3.Stop()
	_ = replFS.Backends["b3"].Close()

	// Wait for monitor to mark b3 down
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 1*time.Second, 20*time.Millisecond)

	// Write should succeed because write quorum of 2 is met (b1, b2)
	content := []byte("quorum test content")
	err := writeFile(ctx, dir, "quorum.txt", content)
	require.NoError(t, err)

	// Verify s1 and s2 have it, s3 does not
	f1, err := s1.MemFS.Open("/quorum.txt")
	require.NoError(t, err)
	_ = f1.Close()
	f2, err := s2.MemFS.Open("/quorum.txt")
	require.NoError(t, err)
	_ = f2.Close()
	_, err = s3.MemFS.Open("/quorum.txt")
	require.Error(t, err)

	// Now stop s2, leaving only s1 alive
	s2.Stop()
	_ = replFS.Backends["b2"].Close()
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b2")
	}, 1*time.Second, 20*time.Millisecond)

	// Write should fail because write quorum of 2 is not met
	err = writeFile(ctx, dir, "failed_quorum.txt", []byte("should fail"))
	require.Error(t, err)
}

func TestIntegration_TombstoneDeletion(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	defer s1.Stop()
	s2 := startTestSMBServer(t, "share2")
	defer s2.Stop()
	s3 := startTestSMBServer(t, "share3")
	defer s3.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replFS, _, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 3)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// Write file
	err := writeFile(ctx, dir, "deleteme.txt", []byte("some content"))
	require.NoError(t, err)

	// Remove it
	err = dir.Remove(ctx, &fuse.RemoveRequest{Name: "deleteme.txt"})
	require.NoError(t, err)

	// Lookup should return EIO or not exist
	_, err = dir.Lookup(ctx, "deleteme.txt")
	require.Error(t, err)

	// Verify deletion on underlying backends and presence of tombstone sidecars
	for i, s := range []*TestSMBServer{s1, s2, s3} {
		_, err := s.MemFS.Open("/deleteme.txt")
		require.Error(t, err, "file still exists on server %d", i+1)

		sidecar, err := vfs.ReadMeta(ctx, replFS.Backends[fmt.Sprintf("b%d", i+1)], "deleteme.txt")
		require.NoError(t, err)
		assert.True(t, sidecar.Deleted)
	}
}

func TestIntegration_Repair(t *testing.T) {
	s1 := startTestSMBServer(t, "share1")
	defer s1.Stop()
	s2 := startTestSMBServer(t, "share2")
	defer s2.Stop()
	s3 := startTestSMBServer(t, "share3")
	defer s3.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write Quorum = 2
	replFS, monitor, cleanup := setupRepliFS(t, ctx, cancel, s1, s2, s3, 2)
	defer cleanup()

	rootNode, _ := replFS.Root()
	dir := rootNode.(*rfuse.Dir)

	// 1. Stop s3
	s3.Stop()
	_ = replFS.Backends["b3"].Close()
	assert.Eventually(t, func() bool {
		return !monitor.IsHealthy("b3")
	}, 1*time.Second, 20*time.Millisecond)

	// 2. Write file (written to s1 and s2)
	content := []byte("repair data content")
	err := writeFile(ctx, dir, "repairme.txt", content)
	require.NoError(t, err)

	// 3. Restart s3
	err = s3.Restart()
	require.NoError(t, err)

	// 4. Wait for monitor to mark b3 healthy
	assert.Eventually(t, func() bool {
		return monitor.IsHealthy("b3")
	}, 2*time.Second, 50*time.Millisecond)

	// 5. Start RepairManager with a very short interval
	repairManager := rfuse.NewRepairManager(replFS, 50*time.Millisecond, 2)
	repairManager.Start(ctx)

	// 6. Wait for file to be repaired onto s3
	assert.Eventually(t, func() bool {
		f, err := s3.MemFS.Open("/repairme.txt")
		if err != nil {
			return false
		}
		defer f.Close()
		b, err := io.ReadAll(f)

		return err == nil && string(b) == string(content)
	}, 5*time.Second, 100*time.Millisecond)
}

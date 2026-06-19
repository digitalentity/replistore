package tests

import (
	"context"
	"fmt"
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
	"github.com/stretchr/testify/require"
)

const (
	testUser = "testuser"
	testPass = "testpass"
)

// TestSMBServer is an in-process SMB server backed by an in-memory filesystem.
// Port holds the actual bound port, read back from the listener so callers never
// race a separately probed free port against the server's own bind.
type TestSMBServer struct {
	Server  *smbfs.Server
	MemFS   *memfs.FileSystem
	Port    int
	Share   string
	stopped bool
	mu      sync.Mutex
}

func serverOptions(port int) smbfs.ServerOptions {
	return smbfs.ServerOptions{
		Port:            port,
		Hostname:        "127.0.0.1",
		AllowGuest:      true,
		SigningRequired: false,
		Users:           map[string]string{testUser: testPass},
	}
}

func shareOptions(name string) smbfs.ShareOptions {
	return smbfs.ShareOptions{
		ShareName:  name,
		SharePath:  "/",
		AllowGuest: true,
	}
}

// freePort probes for an OS-assigned free port by binding :0 and reading the
// chosen port back. smbfs treats port 0 as "use the default" (445), so the
// server cannot bind ephemerally itself; the caller must hand it a concrete
// port. The probe is inherently racy — another listener may claim the port
// between close and the server's bind — so callers retry on a fresh port when
// the bind loses that race.
func freePort() (int, error) {
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// listenOnFreePort starts a server on a probed free port, retrying on a new port
// if the bind loses the probe-to-bind race. Returning the actual bound port lets
// clients connect without separately probing (and possibly disagreeing on) it.
func listenOnFreePort(mfs *memfs.FileSystem, shareName string) (*smbfs.Server, int, error) {
	const attempts = 20

	var lastErr error
	for range attempts {
		port, err := freePort()
		if err != nil {
			lastErr = err

			continue
		}
		server, err := smbfs.NewServer(serverOptions(port))
		if err != nil {
			return nil, 0, fmt.Errorf("new server: %w", err)
		}
		if err := server.AddShare(mfs, shareOptions(shareName)); err != nil {
			return nil, 0, fmt.Errorf("add share: %w", err)
		}
		if err := server.Listen(); err != nil {
			lastErr = err

			continue
		}

		return server, port, nil
	}

	return nil, 0, fmt.Errorf("bind a free port after %d attempts: %w", attempts, lastErr)
}

func startTestSMBServer(t *testing.T, shareName string) *TestSMBServer {
	t.Helper()
	mfs, err := memfs.NewFS()
	require.NoError(t, err)

	server, port, err := listenOnFreePort(mfs, shareName)
	require.NoError(t, err)

	s := &TestSMBServer{
		Server: server,
		MemFS:  mfs,
		Port:   port,
		Share:  shareName,
	}
	t.Cleanup(s.Stop)

	return s
}

// addrOf returns the host:port a client uses to reach the server.
func addrOf(s *TestSMBServer) string {
	return fmt.Sprintf("127.0.0.1:%d", s.Port)
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

// Restart brings the server back up on its original port, reusing the same
// in-memory filesystem so previously written content survives. Rebinding the
// same port can transiently fail while the OS releases the old listener, so the
// bind is retried with a short backoff rather than a single fixed sleep.
func (s *TestSMBServer) Restart() error {
	s.Stop()

	s.mu.Lock()
	s.stopped = false
	s.mu.Unlock()

	const (
		attempts = 50
		backoff  = 20 * time.Millisecond
	)

	var lastErr error
	for range attempts {
		server, err := smbfs.NewServer(serverOptions(s.Port))
		if err != nil {
			return fmt.Errorf("new server: %w", err)
		}
		if err := server.AddShare(s.MemFS, shareOptions(s.Share)); err != nil {
			return fmt.Errorf("add share: %w", err)
		}
		if err := server.Listen(); err != nil {
			lastErr = err
			time.Sleep(backoff)

			continue
		}
		s.Server = server

		return nil
	}

	return fmt.Errorf("rebind port %d after %d attempts: %w", s.Port, attempts, lastErr)
}

func setupRepliFS(t *testing.T, ctx context.Context, cancel context.CancelFunc, s1, s2, s3 *TestSMBServer, writeQuorum int) (*rfuse.RepliFS, *backend.HealthMonitor, func()) {
	t.Helper()
	servers := []*TestSMBServer{s1, s2, s3}
	backends := make(map[string]backend.Backend, len(servers))
	closers := make([]func() error, 0, len(servers))

	for i, s := range servers {
		name := fmt.Sprintf("b%d", i+1)
		addr := fmt.Sprintf("127.0.0.1:%d", s.Port)
		b := smb.NewSMBBackend(name, addr, s.Share, testUser, testPass, "", 10, nil)
		require.NoError(t, b.Connect(ctx))
		backends[name] = b
		closers = append(closers, b.Close)
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
		time.Sleep(50 * time.Millisecond) // Let background loops observe cancellation and exit.
		for _, closeFn := range closers {
			_ = closeFn()
		}
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

package tests

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// pausableProxy is a TCP relay between a client and a target server whose byte
// forwarding can be paused on demand. Pausing strands any in-flight request on
// the wire: the client has sent it but the server never sees it, so the client
// blocks awaiting a response. That models a stalled network — the only way to
// exercise per-call I/O cancellation, since the in-memory SMB server otherwise
// answers instantly.
type pausableProxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
	conns  []net.Conn
	closed bool
}

func startPausableProxy(t *testing.T, target string) *pausableProxy {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &pausableProxy{ln: ln, target: target}
	p.cond = sync.NewCond(&p.mu)
	t.Cleanup(p.Close)

	go p.serve()

	return p
}

func (p *pausableProxy) Addr() string {
	return p.ln.Addr().String()
}

func (p *pausableProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		var d net.Dialer
		server, err := d.DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = client.Close()

			continue
		}
		p.track(client, server)

		go p.pipe(server, client)
		go p.pipe(client, server)
	}
}

func (p *pausableProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		for _, c := range conns {
			_ = c.Close()
		}

		return
	}
	p.conns = append(p.conns, conns...)
}

// pipe copies src into dst, holding each chunk at the gate while paused so that
// a stranded request is never delivered until forwarding resumes.
func (p *pausableProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			p.gate()
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				_ = dst.Close()
			}

			return
		}
	}
}

// gate blocks while the proxy is paused.
func (p *pausableProxy) gate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.paused && !p.closed {
		p.cond.Wait()
	}
}

func (p *pausableProxy) Pause() {
	p.mu.Lock()
	p.paused = true
	p.mu.Unlock()
}

func (p *pausableProxy) Resume() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
	p.cond.Broadcast()
}

func (p *pausableProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return
	}
	p.closed = true
	p.paused = false
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()

	p.cond.Broadcast()
	_ = p.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

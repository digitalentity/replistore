package cluster

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	bmock "github.com/digitalentity/replistore/internal/backend/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newEntryFile returns a MockFile that serves the JSON encoding of the given
// peer entry on ReadAt.
func newEntryFile(t *testing.T, id, addr string, seq int64) *bmock.MockFile {
	t.Helper()
	data, err := json.Marshal(peerEntry{ID: id, Address: addr, Seq: seq})
	require.NoError(t, err)

	f := &bmock.MockFile{}
	f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), data)
	}).Return(len(data), io.EOF)
	f.On("Close").Return(nil)

	return f
}

// expectEntryListing makes the backend list and serve the given peer entries.
func expectEntryListing(t *testing.T, b *bmock.MockBackend, entries ...peerEntry) {
	t.Helper()
	infos := make([]backend.FileInfo, 0, len(entries))
	for _, e := range entries {
		infos = append(infos, backend.FileInfo{Name: e.ID + ".json", Size: 64})
		b.On("OpenFile", mock.Anything, peersDir+"/"+e.ID+".json", os.O_RDONLY, os.FileMode(0)).
			Return(newEntryFile(t, e.ID, e.Address, e.Seq), nil)
	}
	b.On("ReadDir", mock.Anything, peersDir).Return(infos, nil)
}

func TestDiscovery_StartWritesEntryToAllBackends(t *testing.T) {
	ctx := t.Context()

	backends := make([]backend.Backend, 0, 2)
	files := make([]*bmock.MockFile, 0, 2)
	mocks := make([]*bmock.MockBackend, 0, 2)
	for _, name := range []string{"b1", "b2"} {
		f := &bmock.MockFile{}
		f.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Return(64, nil)
		f.On("Close").Return(nil)

		b := &bmock.MockBackend{NameVal: name}
		b.On("MkdirAll", mock.Anything, peersDir, os.FileMode(0755)).Return(nil)
		b.On("OpenFile", mock.Anything, peersDir+"/node1.json", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).
			Return(f, nil)

		backends = append(backends, b)
		files = append(files, f)
		mocks = append(mocks, b)
	}

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, backends)
	err := d.Start(ctx)
	require.NoError(t, err)

	for i := range mocks {
		mocks[i].AssertExpectations(t)
		files[i].AssertExpectations(t)
	}
}

func TestDiscovery_HeartbeatWriteFailureIsTolerated(t *testing.T) {
	f := &bmock.MockFile{}
	f.On("WriteAt", mock.Anything, mock.Anything, int64(0)).Return(64, nil)
	f.On("Close").Return(nil)

	good := &bmock.MockBackend{NameVal: "good"}
	good.On("MkdirAll", mock.Anything, peersDir, os.FileMode(0755)).Return(nil)
	good.On("OpenFile", mock.Anything, peersDir+"/node1.json", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.FileMode(0644)).
		Return(f, nil)

	bad := &bmock.MockBackend{NameVal: "bad"}
	bad.On("MkdirAll", mock.Anything, peersDir, os.FileMode(0755)).Return(assert.AnError)

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{bad, good})
	d.heartbeatOnce(context.Background())

	good.AssertExpectations(t)
	bad.AssertExpectations(t)
}

func TestDiscovery_PollMergesAcrossBackends(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	expectEntryListing(t, b1,
		peerEntry{ID: "node2", Address: "10.0.0.2:5050", Seq: 100},
		peerEntry{ID: "node3", Address: "10.0.0.3:5050", Seq: 50},
	)

	// node2 also present on b2 with a higher seq and different address.
	b2 := &bmock.MockBackend{NameVal: "b2"}
	expectEntryListing(t, b2,
		peerEntry{ID: "node2", Address: "10.0.0.22:5050", Seq: 200},
	)

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{b1, b2})
	d.pollOnce(context.Background())

	peers := d.GetPeers()
	assert.Len(t, peers, 2)

	p2, ok := d.GetPeer("node2")
	assert.True(t, ok)
	assert.Equal(t, "10.0.0.22:5050", p2.Address) // highest seq wins

	_, ok = d.GetPeer("node3")
	assert.True(t, ok)
}

// serveEntry makes the backend list and serve a single peerEntry verbatim
// (including its Sig field), so tests can present forged signatures.
func serveEntry(t *testing.T, b *bmock.MockBackend, e peerEntry) {
	t.Helper()
	data, err := json.Marshal(e)
	require.NoError(t, err)

	f := &bmock.MockFile{}
	f.On("ReadAt", mock.Anything, mock.Anything, int64(0)).Run(func(args mock.Arguments) {
		copy(args.Get(1).([]byte), data)
	}).Return(len(data), io.EOF)
	f.On("Close").Return(nil)

	b.On("ReadDir", mock.Anything, peersDir).Return([]backend.FileInfo{{Name: e.ID + ".json", Size: 64}}, nil)
	b.On("OpenFile", mock.Anything, peersDir+"/"+e.ID+".json", os.O_RDONLY, os.FileMode(0)).Return(f, nil)
}

func TestDiscovery_AcceptsValidlySignedPeerEntry(t *testing.T) {
	secret := []byte("test-cluster-secret-0123456789")
	b := &bmock.MockBackend{NameVal: "b1"}
	serveEntry(t, b, peerEntry{
		ID:      "node2",
		Address: "10.0.0.2:5050",
		Seq:     100,
		Sig:     entryMAC(secret, "node2", "10.0.0.2:5050", 100),
	})

	d := NewDiscovery("node1", "10.0.0.1:5050", secret, []backend.Backend{b})
	d.pollOnce(context.Background())

	_, ok := d.GetPeer("node2")
	assert.True(t, ok)
}

func TestDiscovery_RejectsForgedPeerEntry(t *testing.T) {
	secret := []byte("test-cluster-secret-0123456789")

	t.Run("bad signature", func(t *testing.T) {
		b := &bmock.MockBackend{NameVal: "b1"}
		serveEntry(t, b, peerEntry{ID: "evil", Address: "10.6.6.6:5050", Seq: 100, Sig: "not-a-valid-mac"})

		d := NewDiscovery("node1", "10.0.0.1:5050", secret, []backend.Backend{b})
		d.pollOnce(context.Background())

		_, ok := d.GetPeer("evil")
		assert.False(t, ok)
	})

	t.Run("address tampered after signing", func(t *testing.T) {
		b := &bmock.MockBackend{NameVal: "b1"}
		// Signature valid for the real address; attacker swaps the address to
		// redirect lock RPCs without re-signing.
		serveEntry(t, b, peerEntry{
			ID:      "node2",
			Address: "10.6.6.6:5050",
			Seq:     100,
			Sig:     entryMAC(secret, "node2", "10.0.0.2:5050", 100),
		})

		d := NewDiscovery("node1", "10.0.0.1:5050", secret, []backend.Backend{b})
		d.pollOnce(context.Background())

		_, ok := d.GetPeer("node2")
		assert.False(t, ok)
	})

	t.Run("unsigned entry rejected when secret configured", func(t *testing.T) {
		b := &bmock.MockBackend{NameVal: "b1"}
		serveEntry(t, b, peerEntry{ID: "node2", Address: "10.0.0.2:5050", Seq: 100})

		d := NewDiscovery("node1", "10.0.0.1:5050", secret, []backend.Backend{b})
		d.pollOnce(context.Background())

		_, ok := d.GetPeer("node2")
		assert.False(t, ok)
	})
}

func TestDiscovery_PeerExpiresWhenSeqUnchanged(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	expectEntryListing(t, b1, peerEntry{ID: "node2", Address: "10.0.0.2:5050", Seq: 100})

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{b1})
	d.pollOnce(context.Background())

	_, ok := d.GetPeer("node2")
	assert.True(t, ok)

	// Pretend the last seq change was observed long ago (but within purge window).
	d.mu.Lock()
	d.states["node2"].lastChanged = time.Now().Add(-time.Minute)
	d.mu.Unlock()

	d.pollOnce(context.Background())

	_, ok = d.GetPeer("node2")
	assert.False(t, ok, "peer with stale seq should be removed from Peers")

	// Tracking state is kept so the peer can come back with a fresh seq.
	d.mu.RLock()
	_, tracked := d.states["node2"]
	d.mu.RUnlock()
	assert.True(t, tracked)
}

func TestDiscovery_PeerRemovedWhenEntryDeleted(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("ReadDir", mock.Anything, peersDir).
		Return([]backend.FileInfo{{Name: "node2.json", Size: 64}}, nil).Once()
	b1.On("OpenFile", mock.Anything, peersDir+"/node2.json", os.O_RDONLY, os.FileMode(0)).
		Return(newEntryFile(t, "node2", "10.0.0.2:5050", 100), nil).Once()
	// Second poll: entry deleted from the backend.
	b1.On("ReadDir", mock.Anything, peersDir).Return([]backend.FileInfo{}, nil)

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{b1})

	d.pollOnce(context.Background())
	_, ok := d.GetPeer("node2")
	assert.True(t, ok)

	d.pollOnce(context.Background())
	_, ok = d.GetPeer("node2")
	assert.False(t, ok, "peer absent from all backends should be removed immediately")
}

func TestDiscovery_SkipsOwnEntry(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	expectEntryListing(t, b1, peerEntry{ID: "node1", Address: "10.0.0.1:5050", Seq: 100})

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{b1})
	d.pollOnce(context.Background())

	assert.Empty(t, d.GetPeers())
}

func TestDiscovery_StopIsIdempotentAndRemovesEntry(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("Remove", mock.Anything, peersDir+"/node1.json").Return(nil).Once()

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{b1})
	d.Stop()
	d.Stop() // must not panic or remove twice

	b1.AssertExpectations(t)
}

func TestDiscovery_AllBackendsUnreachableKeepsMembership(t *testing.T) {
	b1 := &bmock.MockBackend{NameVal: "b1"}
	b1.On("ReadDir", mock.Anything, peersDir).
		Return([]backend.FileInfo{{Name: "node2.json", Size: 64}}, nil).Once()
	b1.On("OpenFile", mock.Anything, peersDir+"/node2.json", os.O_RDONLY, os.FileMode(0)).
		Return(newEntryFile(t, "node2", "10.0.0.2:5050", 100), nil).Once()
	// Second poll: the backend is unreachable.
	b1.On("ReadDir", mock.Anything, peersDir).Return([]backend.FileInfo(nil), io.EOF)

	d := NewDiscovery("node1", "10.0.0.1:5050", nil, []backend.Backend{b1})

	d.pollOnce(context.Background())
	_, ok := d.GetPeer("node2")
	assert.True(t, ok)

	d.pollOnce(context.Background())
	_, ok = d.GetPeer("node2")
	assert.True(t, ok, "a poll with zero reachable backends must not mutate membership")
}

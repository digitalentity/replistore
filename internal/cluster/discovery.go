package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/digitalentity/replistore/internal/backend"
	"github.com/sirupsen/logrus"
)

const (
	heartbeatInterval = 10 * time.Second
	pollInterval      = 10 * time.Second
	peerExpiry        = 35 * time.Second
	purgeAfter        = 10 * time.Minute
	peersDir          = ".replistore/peers"

	backendOpTimeout = 5 * time.Second
)

// Peer represents an active RepliStore node discovered via the backends.
type Peer struct {
	ID       string
	Address  string
	LastSeen time.Time
}

// peerEntry is the JSON document each node maintains at
// .replistore/peers/<nodeID>.json on every backend.
type peerEntry struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Seq     int64  `json:"seq"`
}

// peerState tracks liveness of a peer on the reader's local clock.
type peerState struct {
	lastSeq     int64
	lastChanged time.Time
}

// Discovery implements backend-based node discovery: each node heartbeats its
// own entry to all backends and polls the peers directory to learn membership.
type Discovery struct {
	NodeID        string
	AdvertiseAddr string
	Peers         map[string]Peer
	mu            sync.RWMutex

	backends []backend.Backend
	states   map[string]*peerState

	stopCh   chan struct{}
	stopOnce sync.Once
	log      *logrus.Entry
}

func NewDiscovery(nodeID, advertiseAddr string, backends []backend.Backend) *Discovery {
	return &Discovery{
		NodeID:        nodeID,
		AdvertiseAddr: advertiseAddr,
		Peers:         make(map[string]Peer),
		backends:      backends,
		states:        make(map[string]*peerState),
		stopCh:        make(chan struct{}),
		log:           logrus.WithField("component", "discovery").WithField("node_id", nodeID),
	}
}

// Start writes the initial peer entry to all backends and starts the
// heartbeat and poll loops. It fails only if the entry could not be written
// to any backend.
func (d *Discovery) Start(ctx context.Context) error {
	var lastErr error
	okCount := 0
	for _, b := range d.backends {
		if err := d.writeEntry(ctx, b); err != nil {
			d.log.WithError(err).Warnf("Failed to write peer entry to backend %s", b.GetName())
			lastErr = err
			continue
		}
		okCount++
	}
	if len(d.backends) > 0 && okCount == 0 {
		return fmt.Errorf("failed to write peer entry to any backend: %w", lastErr)
	}

	go d.heartbeatLoop(ctx)
	go d.pollLoop(ctx)
	return nil
}

// Stop is idempotent: it stops the loops and best-effort deletes the node's
// own entry from all backends.
func (d *Discovery) Stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)

		ctx, cancel := context.WithTimeout(context.Background(), backendOpTimeout)
		defer cancel()
		entryPath := d.entryPath(d.NodeID)
		for _, b := range d.backends {
			if err := b.Remove(ctx, entryPath); err != nil {
				d.log.WithError(err).Debugf("Failed to remove peer entry from backend %s", b.GetName())
			}
		}
	})
}

func (d *Discovery) GetPeers() []Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()

	res := make([]Peer, 0, len(d.Peers))
	for _, p := range d.Peers {
		res = append(res, p)
	}
	return res
}

func (d *Discovery) GetPeer(id string) (Peer, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	p, ok := d.Peers[id]
	return p, ok
}

func (d *Discovery) entryPath(nodeID string) string {
	return path.Join(peersDir, nodeID+".json")
}

func (d *Discovery) writeEntry(ctx context.Context, b backend.Backend) error {
	opCtx, cancel := context.WithTimeout(ctx, backendOpTimeout)
	defer cancel()

	data, err := json.Marshal(peerEntry{
		ID:      d.NodeID,
		Address: d.AdvertiseAddr,
		Seq:     time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}

	if err := b.MkdirAll(opCtx, peersDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", peersDir, err)
	}

	f, err := b.OpenFile(opCtx, d.entryPath(d.NodeID), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(opCtx, data, 0); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (d *Discovery) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.heartbeatOnce(ctx)
		}
	}
}

func (d *Discovery) heartbeatOnce(ctx context.Context) {
	for _, b := range d.backends {
		if err := d.writeEntry(ctx, b); err != nil {
			d.log.WithError(err).Warnf("Heartbeat write failed on backend %s", b.GetName())
		}
	}
}

func (d *Discovery) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.pollOnce(ctx)
		}
	}
}

// pollOnce lists the peers directory on all backends, merges the entries
// (highest seq per node wins) and updates membership.
func (d *Discovery) pollOnce(ctx context.Context) {
	observed := make(map[string]peerEntry)
	listedOK := 0

	for _, b := range d.backends {
		entries, err := d.readBackendEntries(ctx, b)
		if err != nil {
			d.log.WithError(err).Warnf("Failed to list peers on backend %s", b.GetName())
			continue
		}
		listedOK++
		for _, e := range entries {
			if cur, ok := observed[e.ID]; !ok || e.Seq > cur.Seq {
				observed[e.ID] = e
			}
		}
	}

	// If no backend could be listed we have no information at all; mutating
	// membership now (especially the absent-from-all-backends removal below)
	// would drop every peer over a transient backend outage.
	if listedOK == 0 && len(d.backends) > 0 {
		return
	}

	now := time.Now()
	var toPurge []string

	d.mu.Lock()

	// Update or add observed peers.
	for id, e := range observed {
		st, ok := d.states[id]
		if !ok || st.lastSeq != e.Seq {
			// New peer or fresh heartbeat: (re)admit.
			d.states[id] = &peerState{lastSeq: e.Seq, lastChanged: now}
			d.Peers[id] = Peer{ID: id, Address: e.Address, LastSeen: now}
			continue
		}
		// Seq unchanged: check expiry and purge windows.
		age := now.Sub(st.lastChanged)
		if age > peerExpiry {
			if _, present := d.Peers[id]; present {
				delete(d.Peers, id)
				d.log.Debugf("Peer %s expired (no heartbeat for %v)", id, age)
			}
		}
		if age > purgeAfter {
			toPurge = append(toPurge, id)
			delete(d.states, id)
		}
	}

	// Peers absent from all backends were deleted (graceful shutdown).
	for id := range d.states {
		if _, ok := observed[id]; !ok {
			delete(d.Peers, id)
			delete(d.states, id)
			d.log.Debugf("Peer %s left the cluster (entry removed)", id)
		}
	}

	d.mu.Unlock()

	// Janitor: remove crash-leftover entries outside the lock.
	for _, id := range toPurge {
		d.purgeEntry(ctx, id)
	}
}

// readBackendEntries lists and parses all peer entries on one backend,
// skipping the node's own entry and malformed files.
func (d *Discovery) readBackendEntries(ctx context.Context, b backend.Backend) ([]peerEntry, error) {
	opCtx, cancel := context.WithTimeout(ctx, backendOpTimeout)
	defer cancel()

	infos, err := b.ReadDir(opCtx, peersDir)
	if err != nil {
		return nil, err
	}

	var entries []peerEntry
	for _, fi := range infos {
		if fi.IsDir || !strings.HasSuffix(fi.Name, ".json") {
			continue
		}
		entry, err := d.readEntry(opCtx, b, path.Join(peersDir, fi.Name))
		if err != nil {
			d.log.WithError(err).Debugf("Skipping malformed peer entry %s on backend %s", fi.Name, b.GetName())
			continue
		}
		if entry.ID == "" || entry.ID == d.NodeID {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (d *Discovery) readEntry(ctx context.Context, b backend.Backend, p string) (peerEntry, error) {
	var entry peerEntry

	f, err := b.OpenFile(ctx, p, os.O_RDONLY, 0)
	if err != nil {
		return entry, err
	}
	defer f.Close()

	buf := make([]byte, 4096)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && !(err == io.EOF && n > 0) {
		return entry, err
	}

	if err := json.Unmarshal(buf[:n], &entry); err != nil {
		return entry, err
	}
	return entry, nil
}

// purgeEntry best-effort removes a stale peer's entry from all backends.
func (d *Discovery) purgeEntry(ctx context.Context, nodeID string) {
	entryPath := d.entryPath(nodeID)
	for _, b := range d.backends {
		opCtx, cancel := context.WithTimeout(ctx, backendOpTimeout)
		if err := b.Remove(opCtx, entryPath); err != nil {
			d.log.WithError(err).Debugf("Failed to purge stale peer entry %s on backend %s", entryPath, b.GetName())
		}
		cancel()
	}
	d.log.Infof("Purged stale peer entry for %s", nodeID)
}

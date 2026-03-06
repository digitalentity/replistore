package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
	"github.com/sirupsen/logrus"
)

const ServiceType = "_replistore._tcp"

// Peer represents an active RepliStore node discovered via mDNS.
type Peer struct {
	ID       string
	Address  string
	LastSeen time.Time
}

// Discovery handles mDNS registration and browsing.
type Discovery struct {
	NodeID   string
	Port     int
	Peers    map[string]Peer
	mu       sync.RWMutex

	server  *zeroconf.Server
	browser *zeroconf.Resolver
	stopCh  chan struct{}
	log     *logrus.Entry
}

func NewDiscovery(nodeID string, port int) *Discovery {
	return &Discovery{
		NodeID: nodeID,
		Port:   port,
		Peers:  make(map[string]Peer),
		stopCh: make(chan struct{}),
		log:    logrus.WithField("component", "discovery").WithField("node_id", nodeID),
	}
}

// Start registers the local node and starts browsing for peers.
func (d *Discovery) Start() error {
	// 1. Register local service
	server, err := zeroconf.Register(d.NodeID, ServiceType, "local.", d.Port, []string{"txtv=0", "id=" + d.NodeID}, nil)
	if err != nil {
		return fmt.Errorf("failed to register mDNS service: %w", err)
	}
	d.server = server

	// 2. Start browser
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return fmt.Errorf("failed to create mDNS resolver: %w", err)
	}
	d.browser = resolver

	go d.browseLoop()

	return nil
}

func (d *Discovery) Stop() {
	if d.server != nil {
		d.server.Shutdown()
	}
	close(d.stopCh)
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

func (d *Discovery) browseLoop() {
	entries := make(chan *zeroconf.ServiceEntry)
	
	// Browse context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := d.browser.Browse(ctx, ServiceType, "local.", entries)
		if err != nil {
			d.log.WithError(err).Error("mDNS browse failed")
		}
	}()

	// Periodic cleanup of stale peers
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			cancel()
			return
		case entry := <-entries:
			if entry == nil {
				continue
			}
			if entry.Instance == d.NodeID {
				continue // Ignore ourselves
			}

			// Extract address
			var addr string
			if len(entry.AddrIPv4) > 0 {
				addr = net.JoinHostPort(entry.AddrIPv4[0].String(), strconv.Itoa(entry.Port))
			} else if len(entry.AddrIPv6) > 0 {
				addr = net.JoinHostPort(entry.AddrIPv6[0].String(), strconv.Itoa(entry.Port))
			} else {
				continue
			}

			d.mu.Lock()
			d.Peers[entry.Instance] = Peer{
				ID:       entry.Instance,
				Address:  addr,
				LastSeen: time.Now(),
			}
			d.mu.Unlock()
			d.log.Debugf("Discovered peer: %s at %s", entry.Instance, addr)

		case <-ticker.C:
			d.cleanupStale()
		}
	}
}

func (d *Discovery) cleanupStale() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for id, p := range d.Peers {
		if now.Sub(p.LastSeen) > 2*time.Minute {
			delete(d.Peers, id)
			d.log.Debugf("Removed stale peer: %s", id)
		}
	}
}

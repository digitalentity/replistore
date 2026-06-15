package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/backend"
	_ "github.com/digitalentity/replistore/internal/backend/smb"
	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/config"
	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/sirupsen/logrus"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logrus.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logrus.Infof("Starting RepliStore with %d backends (RF=%d, WQ=%d)", len(cfg.Backends), cfg.ReplicationFactor, cfg.WriteQuorum)

	// Initialize backends
	backends := map[string]backend.Backend{}
	backendList := []backend.Backend{}
	for _, bc := range cfg.Backends {
		b, err := backend.Create(bc.Type, bc.Name, bc.ToOptions())
		if err != nil {
			logrus.Errorf("Failed to create backend %s: %v", bc.Name, err)
			continue
		}
		if err := b.Connect(); err != nil {
			logrus.Errorf("Failed to connect to backend %s: %v", bc.Name, err)
			continue
		}
		backends[bc.Name] = b
		backendList = append(backendList, b)
	}

	if len(backends) == 0 {
		logrus.Fatal("No backends connected")
	}

	// Node identity: cluster node ID when clustering is enabled, and the
	// writer identity recorded in version sidecars either way.
	nodeID, _ := os.Hostname()
	if nodeID == "" {
		nodeID = "replistore-" + time.Now().Format("150405")
	}

	// Initialize P2P Lock Manager and Discovery
	var lockMgr *cluster.LockManager
	var disco *cluster.Discovery

	if cfg.ListenAddr != "" {

		lockMgr = cluster.NewLockManager(nodeID)
		lockMgr.ExpectedClusterSize = cfg.ExpectedClusterSize
		lockMgr.Secret = []byte(cfg.ClusterSecret)
		if _, err := lockMgr.Start(cfg.ListenAddr); err != nil {
			logrus.Fatalf("Failed to start lock manager: %v", err)
		}

		disco = cluster.NewDiscovery(nodeID, cfg.AdvertiseAddr, backendList)
		if err := disco.Start(ctx); err != nil {
			logrus.Fatalf("Failed to start discovery: %v", err)
		}
		logrus.Infof("P2P Cluster discovery started. Node ID: %s, Advertise address: %s", nodeID, cfg.AdvertiseAddr)
	}

	// Initialize Health Monitor
	monitor := backend.NewHealthMonitor(backends)
	monitor.Start(ctx, 10*time.Second)

	// Warmup Cache (Background)
	cache := vfs.NewCache()
	go func() {
		logrus.Info("Starting background metadata warmup...")
		cache.Warmup(ctx, backendList)
		logrus.Info("Initial metadata cache warmup complete")
	}()

	// Start background sync
	refreshInterval, err := time.ParseDuration(cfg.CacheRefreshInterval)
	if err != nil {
		logrus.Warnf("Invalid cache_refresh_interval '%s', defaulting to 5m: %v", cfg.CacheRefreshInterval, err)
		refreshInterval = 5 * time.Minute
	}
	if refreshInterval > 0 {
		cache.StartSync(ctx, backendList, refreshInterval)
		logrus.Infof("Background metadata sync started (interval: %v)", refreshInterval)
	}

	// Mount FUSE
	c, err := fuse.Mount(
		cfg.MountPoint,
		fuse.FSName("replistore"),
		fuse.Subtype("replistore"),
	)
	if err != nil {
		logrus.Fatal(err)
	}
	defer c.Close()

	maxIODuration, err := time.ParseDuration(cfg.MaxIODuration)
	if err != nil {
		logrus.Warnf("Invalid max_io_duration '%s', defaulting to 1s: %v", cfg.MaxIODuration, err)
		maxIODuration = 1 * time.Second
	}

	var selector vfs.BackendSelector
	switch cfg.Selector.Type {
	case "space-aware":
		selector = vfs.NewSpaceAwareSelector(backends, monitor, cfg.Selector.WriteAffinity)
	case "first":
		selector = vfs.NewFirstSelector(monitor)
	case "random":
		fallthrough
	default:
		selector = vfs.NewRandomSelector(monitor)
	}

	srv := fs.New(c, nil)
	replFS := &rfuse.RepliFS{
		Cache:             cache,
		Backends:          backends,
		ReplicationFactor: cfg.ReplicationFactor,
		WriteQuorum:       cfg.WriteQuorum,
		Selector:          selector,
		LockManager:       lockMgr,
		Discovery:         disco,
		NodeID:            nodeID,
		MaxIODuration:     maxIODuration,
	}

	// Initialize and start Repair Manager
	repairInterval, err := time.ParseDuration(cfg.RepairInterval)
	if err != nil && cfg.RepairInterval != "" {
		logrus.Warnf("Invalid repair_interval '%s', defaulting to 1h: %v", cfg.RepairInterval, err)
		repairInterval = 1 * time.Hour
	} else if cfg.RepairInterval == "" {
		repairInterval = 1 * time.Hour
	}
	repairManager := rfuse.NewRepairManager(replFS, repairInterval, cfg.RepairConcurrency)
	repairManager.Start(ctx)
	if repairInterval > 0 {
		logrus.Infof("Background repair manager started (interval: %v, concurrency: %d)", repairInterval, cfg.RepairConcurrency)
	}

	// Handle signals for graceful unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logrus.Info("Unmounting...")
		if disco != nil {
			disco.Stop()
		}
		if lockMgr != nil {
			lockMgr.Stop()
		}
		// Cancel before unmount so background scans/repairs stop racing the
		// unmount, then release backend connections. os.Exit below skips
		// deferred cleanup, so close the FUSE conn and backends explicitly.
		cancel()
		fuse.Unmount(cfg.MountPoint)
		_ = c.Close()
		for name, b := range backends {
			if err := b.Close(); err != nil {
				logrus.Warnf("Error closing backend %s: %v", name, err)
			}
		}
		os.Exit(0)
	}()

	logrus.Infof("Mounted at %s", cfg.MountPoint)
	if err := srv.Serve(replFS); err != nil {
		logrus.Fatal(err)
	}
}

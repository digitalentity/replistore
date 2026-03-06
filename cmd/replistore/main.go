package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/backend"
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

	logrus.Infof("Starting RepliStore with %d backends (RF=%d, WQ=%d)", len(cfg.Backends), cfg.ReplicationFactor, cfg.WriteQuorum)

	// Initialize backends
	backends := make(map[string]backend.Backend)
	var backendList []backend.Backend
	for _, bc := range cfg.Backends {
		b := backend.NewSMBBackend(bc.Name, bc.Address, bc.Share, bc.User, bc.Password, bc.Domain)
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

	// Initialize P2P Lock Manager and Discovery
	var lockMgr *cluster.LockManager
	var disco *cluster.Discovery

	if cfg.ListenAddr != "" {
		nodeID, _ := os.Hostname()
		if nodeID == "" {
			nodeID = "replistore-" + time.Now().Format("150405")
		}

		lockMgr = cluster.NewLockManager(nodeID)
		actualAddr, err := lockMgr.Start(cfg.ListenAddr)
		if err != nil {
			logrus.Fatalf("Failed to start lock manager: %v", err)
		}

		_, portStr, _ := net.SplitHostPort(actualAddr)
		port, _ := strconv.Atoi(portStr)

		disco = cluster.NewDiscovery(nodeID, port)
		if err := disco.Start(); err != nil {
			logrus.Fatalf("Failed to start discovery: %v", err)
		}
		logrus.Infof("P2P Cluster discovery started. Node ID: %s, Port: %d", nodeID, port)
	}

	// Initialize Health Monitor
	monitor := backend.NewHealthMonitor(backends)
	monitor.Start(10 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Warmup Cache
	cache := vfs.NewCache()
	logrus.Info("Warming up metadata cache...")
	cache.Warmup(ctx, backendList)
	logrus.Info("Metadata cache warmed up")

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

	srv := fs.New(c, nil)
	replFS := &rfuse.RepliFS{
		Cache:             cache,
		Backends:          backends,
		ReplicationFactor: cfg.ReplicationFactor,
		WriteQuorum:       cfg.WriteQuorum,
		Selector:          vfs.NewRandomSelector(monitor),
		LockManager:       lockMgr,
		Discovery:         disco,
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
		cancel()
		fuse.Unmount(cfg.MountPoint)
		os.Exit(0)
	}()

	logrus.Infof("Mounted at %s", cfg.MountPoint)
	if err := srv.Serve(replFS); err != nil {
		logrus.Fatal(err)
	}
}

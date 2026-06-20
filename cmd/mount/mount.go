// Package mount implements the mount CLI subcommand using the Cobra framework.
package mount

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/api"
	"github.com/digitalentity/replistore/internal/backend"
	_ "github.com/digitalentity/replistore/internal/backend/local" // local backend driver
	_ "github.com/digitalentity/replistore/internal/backend/smb"   // smb backend driver
	"github.com/digitalentity/replistore/internal/cluster"
	"github.com/digitalentity/replistore/internal/config"
	rfuse "github.com/digitalentity/replistore/internal/fuse"
	"github.com/digitalentity/replistore/internal/observability"
	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/spf13/cobra"
)

const (
	defaultMonitorInterval      = 10 * time.Second
	defaultCacheRefreshInterval = 5 * time.Minute
	periodicCacheSaveInterval   = 30 * time.Second
	defaultRepairGrace          = 4 * time.Hour
	defaultWriteLeaseBuffer     = 2 * time.Second
	apiShutdownTimeout          = 5 * time.Second
)

// NewMountCmd creates and returns the mount subcommand.
func NewMountCmd(version string) *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "mount",
		Short: "Mount a RepliStore filesystem",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				slog.Error("Failed to load config", slog.Any("error", err))
				os.Exit(1)
			}

			// Node identity: cluster node ID when clustering is enabled, and the
			// writer identity recorded in version sidecars either way.
			nodeID, _ := os.Hostname()
			if nodeID == "" {
				nodeID = "replistore-" + time.Now().Format("150405")
			}

			// Initialize Observability (slog + Snowflake)
			if err := observability.Init(cfg.Logging.Level, cfg.Logging.Format, nodeID); err != nil {
				slog.Error("Failed to initialize observability", slog.Any("error", err))
				os.Exit(1)
			}

			if err := run(cfg, nodeID, version, configPath); err != nil {
				slog.Error("RepliStore failed", slog.Any("error", err))
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "Path to the configuration file")

	return cmd
}

// buildMountOptions translates the comma-separated mount options string from
// config into FUSE mount options, warning on (and skipping) unknown ones.
func buildMountOptions(options string) []fuse.MountOption {
	mountOpts := []fuse.MountOption{
		fuse.FSName("replistore"),
		fuse.Subtype("replistore"),
	}
	if options == "" {
		return mountOpts
	}
	for part := range strings.SplitSeq(options, ",") {
		opt := strings.TrimSpace(part)
		switch opt {
		case "allow_other":
			mountOpts = append(mountOpts, fuse.AllowOther())
		case "default_permissions":
			mountOpts = append(mountOpts, fuse.DefaultPermissions())
		case "ro", "readonly":
			mountOpts = append(mountOpts, fuse.ReadOnly())
		case "nonempty":
			mountOpts = append(mountOpts, fuse.AllowNonEmptyMount())
		default:
			slog.Warn("Unknown mount option, ignoring", slog.String("option", opt))
		}
	}

	return mountOpts
}

//nolint:gocyclo // main runner wires up the components
func run(cfg *config.Config, nodeID string, version string, configPath string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slog.Info("Starting RepliStore",
		slog.String("version", version),
		slog.Int("backend_count", len(cfg.Backends)),
		slog.Int("replication_factor", cfg.Replication.Factor),
		slog.Int("write_quorum", cfg.Replication.WriteQuorum),
	)

	// Initialize backends
	backends, backendList, err := initBackends(ctx, cfg)
	if err != nil {
		return err
	}

	// Initialize P2P Lock Manager and Discovery
	lockMgr, disco, err := initCluster(ctx, cfg, nodeID, backendList)
	if err != nil {
		return err
	}

	// Initialize Health Monitor
	monitor := backend.NewHealthMonitor(backends)
	monitor.Start(ctx, defaultMonitorInterval)

	// Start background sync config
	refreshInterval, err := time.ParseDuration(cfg.Cache.RefreshInterval)
	if err != nil {
		slog.Warn("Invalid cache_refresh_interval, defaulting to 5m",
			slog.String("interval", cfg.Cache.RefreshInterval),
			slog.Any("error", err),
		)
		refreshInterval = defaultCacheRefreshInterval
	}

	// Warmup/Load Cache (Background & Disk-backed)
	cache := vfs.NewCache()
	cacheFile := filepath.Join(cfg.Cache.StateDir, "cache.json")

	loadedFromDisk, cacheFresh := loadCacheFromDisk(cfg.Cache.StateDir, cacheFile, cache, refreshInterval)

	go func() {
		if cacheFresh {
			return
		}
		if loadedFromDisk {
			slog.Info("Metadata cache loaded from disk is stale. Starting background validation scan...")
		} else {
			slog.Info("Starting background metadata warmup...")
		}
		cache.Warmup(ctx, backendList)
		slog.Info("Metadata cache warmup/validation complete")
		if err := cache.SaveToFile(cacheFile); err != nil {
			slog.Error("Failed to save metadata cache to disk", slog.Any("error", err))
		}
	}()

	// Periodic cache save to disk (every 30 seconds)
	go func() {
		ticker := time.NewTicker(periodicCacheSaveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := cache.SaveToFile(cacheFile); err != nil {
					slog.Error("Failed to periodically save metadata cache to disk", slog.Any("error", err))
				} else {
					slog.Debug("Periodically saved metadata cache to disk")
				}
			}
		}
	}()

	if refreshInterval > 0 {
		cache.StartSync(ctx, backendList, refreshInterval)
		slog.Info("Background metadata sync started", slog.Duration("interval", refreshInterval))
	}

	// Mount FUSE
	mountOpts := buildMountOptions(cfg.Mount.Options)

	c, err := fuse.Mount(
		cfg.Mount.Path,
		mountOpts...,
	)
	if err != nil {
		slog.Error("FUSE mount failed", slog.Any("error", err))

		return fmt.Errorf("FUSE mount failed: %w", err)
	}
	defer c.Close()

	writeLeaseBuffer, err := time.ParseDuration(cfg.WriteLeaseBuffer)
	if err != nil {
		slog.Warn("Invalid write_lease_buffer, defaulting to 2s",
			slog.String("duration", cfg.WriteLeaseBuffer),
			slog.Any("error", err),
		)
		writeLeaseBuffer = defaultWriteLeaseBuffer
	}

	attrValid, err := time.ParseDuration(cfg.Mount.AttrValid)
	if err != nil {
		slog.Warn("Invalid attr_valid, defaulting to 1s",
			slog.String("duration", cfg.Mount.AttrValid),
			slog.Any("error", err),
		)
		attrValid = time.Second
	}

	selector := initSelector(cfg, backends, monitor)

	srv := fs.New(c, nil)
	replFS := &rfuse.RepliFS{
		Cache:             cache,
		Backends:          backends,
		ReplicationFactor: cfg.Replication.Factor,
		WriteQuorum:       cfg.Replication.WriteQuorum,
		Selector:          selector,
		LockManager:       lockMgr,
		Discovery:         disco,
		NodeID:            nodeID,
		WriteLeaseBuffer:  writeLeaseBuffer,
		CacheTTL:          refreshInterval,
		HealthMonitor:     monitor,
		AttrValid:         attrValid,
		UID:               cfg.Mount.UID,
		GID:               cfg.Mount.GID,
	}

	// Initialize and start Repair Manager
	repairMgr := initRepairManager(ctx, cfg, replFS)

	// Initialize and start HTTP API Server if configured
	if cfg.API.Addr != "" {
		apiSrv := api.NewServer(cfg.API.Addr, cfg.API.APIToken, cfg.API.MetricsToken, replFS, repairMgr, configPath, version)
		if err := apiSrv.Start(); err != nil {
			slog.Error("Failed to start HTTP API server", slog.Any("error", err))

			return fmt.Errorf("failed to start HTTP API server: %w", err)
		}
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), apiShutdownTimeout)
			defer shutdownCancel()
			if err := apiSrv.Stop(shutdownCtx); err != nil {
				slog.Error("Failed to gracefully stop HTTP API server", slog.Any("error", err))
			}
		}()
	}

	// Handle signals for graceful unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Unmounting...")
		if disco != nil {
			disco.Stop()
		}
		if lockMgr != nil {
			lockMgr.Stop()
		}
		cancel()

		slog.Info("Saving metadata cache to disk before exit...")
		if err := cache.SaveToFile(cacheFile); err != nil {
			slog.Error("Failed to save metadata cache to disk at shutdown", slog.Any("error", err))
		} else {
			slog.Info("Metadata cache saved successfully")
		}

		if err := fuse.Unmount(cfg.Mount.Path); err != nil {
			slog.Warn("Failed to unmount", slog.String("mount_point", cfg.Mount.Path), slog.Any("error", err))
		}
		_ = c.Close()
		for name, b := range backends {
			if err := b.Close(); err != nil {
				slog.Warn("Error closing backend", slog.String("backend", name), slog.Any("error", err))
			}
		}
		os.Exit(0)
	}()

	slog.Info("Mounted filesystem", slog.String("mount_point", cfg.Mount.Path))
	if err := srv.Serve(replFS); err != nil {
		return fmt.Errorf("FUSE server stopped with error: %w", err)
	}

	return nil
}

func initBackends(ctx context.Context, cfg *config.Config) (map[string]backend.Backend, []backend.Backend, error) {
	backends := map[string]backend.Backend{}
	backendList := []backend.Backend{}
	for _, bc := range cfg.Backends {
		b, err := backend.Create(bc.Type, bc.Name, bc.ToOptions())
		if err != nil {
			slog.Error("Failed to create backend", slog.String("backend", bc.Name), slog.Any("error", err))

			continue
		}
		if err := b.Connect(ctx); err != nil {
			slog.Error("Failed to connect to backend", slog.String("backend", bc.Name), slog.Any("error", err))

			continue
		}
		backends[bc.Name] = b
		backendList = append(backendList, b)
	}

	if len(backends) == 0 {
		return nil, nil, errors.New("no backends connected")
	}

	return backends, backendList, nil
}

func initCluster(ctx context.Context, cfg *config.Config, nodeID string, backendList []backend.Backend) (*cluster.LockManager, *cluster.Discovery, error) {
	var lockMgr *cluster.LockManager
	var disco *cluster.Discovery

	if cfg.Cluster.ListenAddr != "" {
		lockMgr = cluster.NewLockManager(nodeID)
		lockMgr.ExpectedClusterSize = cfg.Cluster.ExpectedClusterSize
		lockMgr.Secret = []byte(cfg.Cluster.Secret)
		if _, err := lockMgr.Start(cfg.Cluster.ListenAddr); err != nil {
			slog.Error("Failed to start lock manager", slog.Any("error", err))

			return nil, nil, fmt.Errorf("failed to start lock manager: %w", err)
		}

		disco = cluster.NewDiscovery(nodeID, cfg.Cluster.AdvertiseAddr, backendList)
		if err := disco.Start(ctx); err != nil {
			slog.Error("Failed to start discovery", slog.Any("error", err))

			return nil, nil, fmt.Errorf("failed to start discovery: %w", err)
		}
		slog.Info("P2P Cluster discovery started",
			slog.String("node_id", nodeID),
			slog.String("advertise_addr", cfg.Cluster.AdvertiseAddr),
		)
	}

	return lockMgr, disco, nil
}

//nolint:ireturn // Factory helper returning BackendSelector interface
func initSelector(cfg *config.Config, backends map[string]backend.Backend, monitor *backend.HealthMonitor) vfs.BackendSelector {
	switch cfg.Selector.Type {
	case "smart":
		return vfs.NewSmartSelector(backends, monitor, cfg.Selector.WriteAffinity)
	case "first":
		return vfs.NewFirstSelector(monitor)
	case "random":
		fallthrough
	default:
		return vfs.NewRandomSelector(monitor)
	}
}

func initRepairManager(ctx context.Context, cfg *config.Config, replFS *rfuse.RepliFS) *rfuse.RepairManager {
	repairInterval, err := time.ParseDuration(cfg.Repair.Interval)
	if err != nil && cfg.Repair.Interval != "" {
		slog.Warn("Invalid repair_interval, defaulting to 1h",
			slog.String("interval", cfg.Repair.Interval),
			slog.Any("error", err),
		)
		repairInterval = 1 * time.Hour
	} else if cfg.Repair.Interval == "" {
		repairInterval = 1 * time.Hour
	}
	repairGrace, err := time.ParseDuration(cfg.Repair.Grace)
	if err != nil && cfg.Repair.Grace != "" {
		slog.Warn("Invalid repair_grace, defaulting to 15m",
			slog.String("grace", cfg.Repair.Grace),
			slog.Any("error", err),
		)
		repairGrace = defaultRepairGrace
	} else if cfg.Repair.Grace == "" {
		repairGrace = defaultRepairGrace
	}
	if repairGrace > 0 && repairGrace < repairInterval {
		slog.Warn("repair_grace is shorter than repair_interval; raising it to the interval",
			slog.Duration("repair_grace", repairGrace),
			slog.Duration("repair_interval", repairInterval),
		)
		repairGrace = repairInterval
	}
	repairManager := rfuse.NewRepairManager(replFS, repairInterval, repairGrace, cfg.Repair.Concurrency)
	repairManager.Start(ctx)
	if repairInterval > 0 {
		slog.Info("Background repair manager started",
			slog.Duration("interval", repairInterval),
			slog.Duration("grace", repairGrace),
			slog.Int("concurrency", cfg.Repair.Concurrency),
		)
	}

	return repairManager
}

func loadCacheFromDisk(stateDir, cacheFile string, cache *vfs.Cache, refreshInterval time.Duration) (bool, bool) {
	if err := os.MkdirAll(stateDir, 0750); err != nil {
		slog.Error("Failed to create state directory", slog.String("state_dir", stateDir), slog.Any("error", err))

		return false, false
	}

	if _, err := os.Stat(cacheFile); err != nil {
		return false, false
	}

	slog.Info("Loading metadata cache from disk", slog.String("cache_file", cacheFile))
	if err := cache.LoadFromFile(cacheFile); err != nil {
		slog.Error("Failed to load metadata cache", slog.Any("error", err))

		return false, false
	}
	slog.Info("Metadata cache loaded successfully from disk")

	cache.Mu.RLock()
	lastRecon := cache.LastReconciled
	cache.Mu.RUnlock()
	if !lastRecon.IsZero() && time.Since(lastRecon) < refreshInterval {
		slog.Info("Loaded cache is fresh. Skipping initial background scan.",
			slog.Time("last_reconciled", lastRecon),
			slog.Duration("threshold", refreshInterval),
		)

		return true, true
	}

	return true, false
}

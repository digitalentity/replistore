package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/digitalentity/replistore/internal/backend"
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

	logrus.Infof("Starting RepliStore with %d backends (RF=%d)", len(cfg.Backends), cfg.ReplicationFactor)

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

	// Warmup Cache
	cache := vfs.NewCache()
	logrus.Info("Warming up metadata cache...")
	cache.Warmup(context.Background(), backendList)
	logrus.Info("Metadata cache warmed up")

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
	}

	// Handle signals for graceful unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logrus.Info("Unmounting...")
		fuse.Unmount(cfg.MountPoint)
		os.Exit(0)
	}()

	logrus.Infof("Mounted at %s", cfg.MountPoint)
	if err := srv.Serve(replFS); err != nil {
		logrus.Fatal(err)
	}
}

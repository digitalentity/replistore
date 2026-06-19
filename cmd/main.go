package main

import (
	"log/slog"
	"os"

	"github.com/digitalentity/replistore/cmd/mount"
	"github.com/spf13/cobra"
)

// Version is the build version of RepliStore, injected at compilation.
var Version = "v0.0.0-unknown"

func main() {
	rootCmd := &cobra.Command{
		Use:     "replistore",
		Short:   "RepliStore is a replicated FUSE filesystem backed by SMB shares.",
		Version: Version,
	}

	rootCmd.AddCommand(mount.NewMountCmd(Version))

	if err := rootCmd.Execute(); err != nil {
		slog.Error("CLI execution failed", slog.Any("error", err))
		os.Exit(1)
	}
}

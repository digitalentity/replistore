package smb

import (
	"context"

	"github.com/cloudsoda/go-smb2"
)

// execute runs op against a context-bound share, transparently reconnecting and
// retrying once if the operation fails with a connection error. It is the single
// entry point for every share-level operation.
func (b *SMBBackend) execute(ctx context.Context, op func(share *smb2.Share) error) error {
	if err := b.ensureConnected(); err != nil {
		return err
	}
	share, err := b.getShare(ctx)
	if err != nil {
		return err
	}

	err = op(share)
	if err != nil && isConnectionError(err) {
		_ = b.Close()
		if reconnectErr := b.ensureConnected(); reconnectErr != nil {
			return err
		}
		share, err = b.getShare(ctx)
		if err != nil {
			return err
		}

		return op(share)
	}

	return err
}

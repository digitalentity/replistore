package fuse_test

import (
	"context"
	"fmt"
	"syscall"
	"testing"

	rerrors "github.com/digitalentity/replistore/internal/errors"
	"github.com/digitalentity/replistore/internal/fuse"
	"github.com/stretchr/testify/assert"
)

func TestTranslateError(t *testing.T) {
	tests := []struct {
		err      error
		expected error
	}{
		{nil, nil},
		{syscall.ENOENT, syscall.ENOENT},
		{fmt.Errorf("wrapped: %w", syscall.EACCES), syscall.EACCES},
		{context.DeadlineExceeded, syscall.ETIMEDOUT},
		{context.Canceled, syscall.EINTR},
		{rerrors.ErrLockConflict, syscall.EAGAIN},
		{fmt.Errorf("wrap: %w", rerrors.ErrLockTimeout), syscall.ETIMEDOUT},
		{rerrors.ErrBackendDown, syscall.EHOSTUNREACH},
		{rerrors.ErrQuorumFailed, syscall.EIO},
		{rerrors.ErrUnavailable, syscall.ENOTCONN},
	}

	for _, tt := range tests {
		actual := fuse.TranslateError(tt.err)
		assert.Equal(t, tt.expected, actual)
	}
}

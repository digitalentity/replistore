package fuse

import (
	"context"
	"errors"
	"syscall"

	rerrors "github.com/digitalentity/replistore/internal/errors"
)

// TranslateError maps RepliStore internal errors to FUSE POSIX error codes.
func TranslateError(err error) error {
	if err == nil {
		return nil
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return syscall.ETIMEDOUT
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}

	if errors.Is(err, rerrors.ErrLockConflict) {
		return syscall.EAGAIN
	}
	if errors.Is(err, rerrors.ErrLockTimeout) {
		return syscall.ETIMEDOUT
	}
	if errors.Is(err, rerrors.ErrBackendDown) {
		return syscall.EHOSTUNREACH
	}
	if errors.Is(err, rerrors.ErrQuorumFailed) {
		return syscall.EIO
	}
	if errors.Is(err, rerrors.ErrUnavailable) {
		return syscall.ENOTCONN
	}

	return err
}

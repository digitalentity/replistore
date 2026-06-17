// Package errors defines domain-specific sentinel errors for RepliStore.
package errors

import "errors"

var (
	ErrBackendDown  = errors.New("errors: backend down")
	ErrQuorumFailed = errors.New("errors: quorum write failed")
	ErrLockConflict = errors.New("errors: lock conflict")
	ErrLockTimeout  = errors.New("errors: lock acquisition timeout")
	ErrUnavailable  = errors.New("errors: no backend available for authoritative answer")
)

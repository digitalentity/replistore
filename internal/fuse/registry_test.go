package fuse

import (
	"testing"

	"github.com/digitalentity/replistore/internal/vfs"
	"github.com/stretchr/testify/assert"
)

func TestHandleRegistry(t *testing.T) {
	var r handleRegistry // zero value must be usable

	n1 := &vfs.Node{}
	n2 := &vfs.Node{}
	h1 := &FileHandle{}
	h2 := &FileHandle{}

	// Empty registry.
	assert.Nil(t, r.forNode(n1))

	// Deregistering an unknown handle is a no-op.
	r.deregister(n1, h1)
	assert.Nil(t, r.forNode(n1))

	r.register(n1, h1)
	r.register(n1, h2)
	r.register(n2, h1)

	assert.Equal(t, []*FileHandle{h1, h2}, r.forNode(n1))
	assert.Equal(t, []*FileHandle{h1}, r.forNode(n2))

	// forNode returns a snapshot; mutating it must not affect the registry.
	snap := r.forNode(n1)
	snap[0] = nil
	assert.Equal(t, []*FileHandle{h1, h2}, r.forNode(n1))

	r.deregister(n1, h1)
	assert.Equal(t, []*FileHandle{h2}, r.forNode(n1))

	r.deregister(n1, h2)
	assert.Nil(t, r.forNode(n1))
	// Empty entry removed from map.
	r.mu.Lock()
	_, ok := r.handles[n1]
	r.mu.Unlock()
	assert.False(t, ok)

	r.deregister(n2, h1)
	assert.Nil(t, r.forNode(n2))
}

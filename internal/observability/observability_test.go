package observability_test

import (
	"context"
	"testing"

	"github.com/digitalentity/replistore/internal/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservability(t *testing.T) {
	err := observability.Init("debug", "json", "test-node")
	require.NoError(t, err)

	t.Run("GenerateCorrelationID produces valid base36 values", func(t *testing.T) {
		id1 := observability.GenerateCorrelationID()
		id2 := observability.GenerateCorrelationID()

		assert.NotEmpty(t, id1)
		assert.NotEmpty(t, id2)
		assert.NotEqual(t, id1, id2)

		// Check if it's alphanumeric lowercase (valid base36)
		for _, r := range id1 {
			assert.True(t, (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
		}
	})

	t.Run("Context storage and retrieval", func(t *testing.T) {
		ctx := context.Background()
		assert.Empty(t, observability.CorrelationID(ctx))

		id := "123abc456"
		ctxWithID := observability.WithCorrelationID(ctx, id)
		assert.Equal(t, id, observability.CorrelationID(ctxWithID))

		logger := observability.Logger(ctxWithID)
		assert.NotNil(t, logger)
	})
}

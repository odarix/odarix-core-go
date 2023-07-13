package delivery_test

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/stretchr/testify/assert"
)

func TestHighAvailability(t *testing.T) {
	t.Log("init ha")
	clock := clockwork.NewFakeClock()
	haTracker := delivery.NewHighAvailabilityTracker(context.Background(), nil, clock)

	t.Log("choose the main replica")
	assert.False(t, haTracker.IsDrop("", "replica1"))
	assert.False(t, haTracker.IsDrop("cluster1", "replica1"))

	t.Log("check replica after 10s")
	clock.Advance(10 * time.Second)
	assert.False(t, haTracker.IsDrop("", "replica1"))
	assert.True(t, haTracker.IsDrop("", "replica2"))
	assert.False(t, haTracker.IsDrop("cluster1", "replica1"))
	assert.True(t, haTracker.IsDrop("cluster1", "replica2"))

	t.Log("check replica after 10s")
	clock.Advance(10 * time.Second)
	assert.False(t, haTracker.IsDrop("", "replica1"))
	assert.True(t, haTracker.IsDrop("cluster1", "replica2"))

	t.Log("check replica after 16s, must new choose replica1")
	clock.Advance(31 * time.Second)
	assert.False(t, haTracker.IsDrop("", "replica1"))
	assert.False(t, haTracker.IsDrop("cluster1", "replica1"))

	t.Log("check replica after 10s")
	clock.Advance(10 * time.Second)
	assert.True(t, haTracker.IsDrop("", "replica2"))
	assert.False(t, haTracker.IsDrop("cluster1", "replica1"))

	t.Log("check replica after 16s, must new choose replica2")
	clock.Advance(31 * time.Second)
	assert.True(t, haTracker.IsDrop("", "replica2"))
	assert.True(t, haTracker.IsDrop("cluster1", "replica2"))
	assert.True(t, haTracker.IsDrop("", "replica1"))
	assert.True(t, haTracker.IsDrop("cluster1", "replica1"))
	assert.False(t, haTracker.IsDrop("", "replica2"))
	assert.False(t, haTracker.IsDrop("cluster1", "replica2"))

	haTracker.Destroy()
}

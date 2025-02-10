package delivery_test

import (
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/odarix/odarix-core-go/delivery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithStartDuration(t *testing.T) {
	ebo := backoff.NewConstantBackOff(100 * time.Millisecond)
	wsdbo := delivery.WithStartDuration(ebo, 0)
	next := wsdbo.NextBackOff()
	assert.Equal(t, 0*time.Millisecond, next)

	next = wsdbo.NextBackOff()
	assert.Equal(t, 100*time.Millisecond, next)

	ebo.Reset()
	wsdbo = delivery.WithStartDuration(ebo, 50*time.Millisecond)
	next = wsdbo.NextBackOff()
	assert.Equal(t, 50*time.Millisecond, next)

	next = wsdbo.NextBackOff()
	assert.Equal(t, 100*time.Millisecond, next)
}

func TestPostRetryWithData(t *testing.T) {
	ebo := backoff.NewConstantBackOff(100 * time.Millisecond)
	wsdbo := delivery.WithStartDuration(ebo, 0)
	baseCtx := context.Background()

	start := time.Now()
	_, err := delivery.PostRetryWithData(baseCtx, func() (*struct{}, error) {
		return nil, nil
	}, wsdbo)
	si := time.Since(start)
	require.NoError(t, err)
	require.InDelta(t, 0, si, float64(10*time.Millisecond))

	start = time.Now()
	_, err = delivery.PostRetryWithData(baseCtx, func() (*struct{}, error) {
		return nil, nil
	}, wsdbo)
	si = time.Since(start)
	require.NoError(t, err)
	require.InDelta(t, 100*time.Millisecond, si, float64(15*time.Millisecond))

	wsdbo.Reset()
	start = time.Now()
	_, err = delivery.PostRetryWithData(baseCtx, func() (*struct{}, error) {
		return nil, nil
	}, wsdbo)
	si = time.Since(start)
	require.NoError(t, err)
	require.InDelta(t, 0, si, float64(10*time.Millisecond))
}

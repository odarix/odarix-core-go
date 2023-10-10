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

	start := time.Now()
	_, err := delivery.PostRetryWithData(context.Background(), func() (*struct{}, error) {
		return nil, nil
	}, wsdbo)
	require.NoError(t, err)
	require.InDelta(t, 0, time.Since(start), float64(10*time.Millisecond))

	start = time.Now()
	_, err = delivery.PostRetryWithData(context.Background(), func() (*struct{}, error) {
		return nil, nil
	}, wsdbo)
	require.NoError(t, err)
	require.InDelta(t, 100*time.Millisecond, time.Since(start), float64(10*time.Millisecond))

	wsdbo.Reset()
	start = time.Now()
	_, err = delivery.PostRetryWithData(context.Background(), func() (*struct{}, error) {
		return nil, nil
	}, wsdbo)
	require.NoError(t, err)
	require.InDelta(t, 0, time.Since(start), float64(10*time.Millisecond))
}

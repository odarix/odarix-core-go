package relabeler

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/odarix/odarix-core-go/cppbridge"
	"github.com/odarix/odarix-core-go/model"
	"github.com/odarix/odarix-core-go/relabeler/block"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewBlock(t *testing.T) {
	lss := cppbridge.NewQueryableLssStorage()
	ds := cppbridge.NewHeadDataStorage()
	ls := model.NewLabelSetBuilder().Set("__name__", "this_is_obviuosly_the_best_metric").Set("lol", "kek").Build()
	lsID := lss.FindOrEmplace(ls)

	enc := cppbridge.NewHeadEncoderWithDataStorage(ds)
	ts := time.Now()
	enc.Encode(lsID, ts.UnixMilli(), 0)
	enc.Encode(lsID, ts.Add(time.Minute).UnixMilli(), 1)
	enc.Encode(lsID, ts.Add(time.Minute*2).UnixMilli(), 2)

	dir, err := os.Getwd()
	require.NoError(t, err)
	t.Log(dir)

	dir = filepath.Join(dir, "data")
	blockWriter := block.NewBlockWriter(dir, block.DefaultChunkSegmentSize, 2*time.Hour, prometheus.DefaultRegisterer)
	err = blockWriter.Write(NewBlock(lss, ds))
	require.NoError(t, err)

}

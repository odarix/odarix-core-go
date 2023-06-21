package internal_test

import (
	"testing"

	"github.com/odarix/odarix-core-go/common"
)

func TestCBindingsInitCleanSmoke(t *testing.T) {
	var encoder = common.NewEncoder(0, 0)
	encoder.Destroy()
	t.Log("TEST")
}

func TestCBindingsEncodeDecode(t *testing.T) {
}

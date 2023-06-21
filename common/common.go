// package common contains shared types (like Decoder, Encoder, Redundant, Segment)
// between delivery, server and related.

// This file contains the init() function for whole package.
package common

import "github.com/odarix/odarix-core-go/common/internal"

func init() {
	internal.Init()
}

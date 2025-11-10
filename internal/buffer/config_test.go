package buffer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/holmberd/go-cmap/internal/testutils"
)

func TestConfigValidate(t *testing.T) {
	pool := &testutils.MockChunkPool{}

	t.Run("Valid config", func(t *testing.T) {
		c := Config{
			P:                     0.5,
			ChunkSize:             testutils.MockChunkSizes[0],
			CompactBytes:          testutils.MockChunkSizes[0],
			CompactDeadRatio:      0.3,
			CompactDeadChunkRatio: 0.5,
		}
		if err := c.Validate(pool); err != nil {
			t.Errorf("expected a valid config, but got error: %v", err)
		}
	})

	t.Run("Invalid P", func(t *testing.T) {
		pValues := []float64{-1.0, 1.1}
		c := Config{
			ChunkSize:             testutils.MockChunkSizes[0],
			CompactBytes:          testutils.MockChunkSizes[0],
			CompactDeadRatio:      0.3,
			CompactDeadChunkRatio: 0.5,
		}

		for _, p := range pValues {
			t.Run(fmt.Sprintf("P = %f", p), func(t *testing.T) {
				c.P = p
				err := c.Validate(pool)
				if err == nil {
					t.Fatal("expected an error for invalid P, but got nil")
				}
				expectedErr := "invalid config: P must be between 0.0 and 1.0"
				if err.Error() != expectedErr {
					t.Errorf("expected error %q, got %q", expectedErr, err.Error())
				}
			})
		}
	})

	t.Run("Invalid chunk size", func(t *testing.T) {
		c := Config{
			P:                     0.5,
			ChunkSize:             testutils.MockChunkSizes[0] - 1, // Invalid chunk size.
			CompactBytes:          testutils.MockChunkSizes[0],
			CompactDeadRatio:      0.3,
			CompactDeadChunkRatio: 0.5,
		}

		err := c.Validate(pool)
		if err == nil {
			t.Fatal("expected an error for invalid chunk size, but got nil")
		}

		expectedErrSubstring := fmt.Sprintf("invalid config: invalid chunk size %d", testutils.MockChunkSizes[0]-1)
		if !strings.Contains(err.Error(), expectedErrSubstring) {
			t.Errorf("expected error to contain %q, got %q", expectedErrSubstring, err.Error())
		}
	})

	t.Run("Multiple invalid fields", func(t *testing.T) {
		c := Config{
			P:                     -1.0,                            // Invalid P.
			ChunkSize:             testutils.MockChunkSizes[0] - 1, // Invalid chunk size.
			CompactBytes:          testutils.MockChunkSizes[0],
			CompactDeadRatio:      0.3,
			CompactDeadChunkRatio: 0.5,
		}

		err := c.Validate(pool)
		if err == nil {
			t.Fatal("expected an error for multiple invalid fields, but got nil")
		}

		errString := err.Error()
		if !strings.Contains(errString, "invalid config: P must be between 0.0 and 1.0") {
			t.Errorf("error message missing expected P validation: got %q", errString)
		}
		if !strings.Contains(
			errString,
			fmt.Sprintf("invalid config: invalid chunk size %d", testutils.MockChunkSizes[0]-1),
		) {
			t.Errorf("error message missing expected chunk size validation: got %q", errString)
		}
	})
}

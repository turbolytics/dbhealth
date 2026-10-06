package main

import (
	"os"
	"testing"

	"github.com/zeebo/assert"
)

// The release verifies an image by running `dbhealth version` on each
// architecture and reading the tag back, so the line is "dbhealth <version>"
// and nothing else.
func TestVersion_PrintsTheBuildsVersion(t *testing.T) {
	r, w, err := os.Pipe()
	assert.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = w
	code := run([]string{"version"})
	os.Stdout = stdout
	w.Close()
	out := make([]byte, 64)
	n, _ := r.Read(out)
	assert.Equal(t, 0, code)
	assert.Equal(t, "dbhealth dev\n", string(out[:n]))
}

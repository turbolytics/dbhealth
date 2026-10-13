package main

import (
	"os"
	"testing"

	"github.com/zeebo/assert"

	"github.com/turbolytics/dbhealth/internal/config"
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

// Each database's name, cluster and labels from the file reach the
// reporter, which sends them in every bundle.
func TestInstanceOf_CarriesTheFilesFields(t *testing.T) {
	db := config.Database{Name: "billing-primary", Cluster: "billing",
		Labels: map[string]string{"client": "acme"}}
	inst := instanceOf(db, nil)
	assert.Equal(t, "billing-primary", inst.Name)
	assert.Equal(t, "billing", inst.Cluster)
	assert.DeepEqual(t, map[string]string{"client": "acme"}, inst.Labels)
}

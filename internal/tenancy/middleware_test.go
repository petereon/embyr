package tenancy_test

import (
	"testing"

	"github.com/petereon/embyr/internal/tenancy"
	"github.com/stretchr/testify/assert"
)

func TestParseTenantKey(t *testing.T) {
	cases := []struct {
		path     string
		project  string
		database string
		ok       bool
	}{
		{"projects/acme/databases/prod/documents/users/alice", "acme", "prod", true},
		{"projects/acme/databases/prod", "acme", "prod", true},
		{"projects/acme/databases/prod/documents", "acme", "prod", true},
		{"", "", "", false},
		{"projects/acme", "", "", false},
		{"notapath", "", "", false},
	}
	for _, tc := range cases {
		p, d, ok := tenancy.ParseTenantKey(tc.path)
		assert.Equal(t, tc.ok, ok, "path=%q", tc.path)
		assert.Equal(t, tc.project, p, "path=%q", tc.path)
		assert.Equal(t, tc.database, d, "path=%q", tc.path)
	}
}

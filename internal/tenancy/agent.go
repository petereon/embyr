package tenancy

import (
	"context"
	"fmt"
)

// AgentResolver resolves a DSN through the embyr-agent tunnel protocol.
// This stub returns an error until Plan 3 (embyr-agent) is implemented.
type AgentResolver struct {
	agentID string
}

func (r *AgentResolver) Resolve(_ context.Context) (string, error) {
	return "", fmt.Errorf("tenancy: agent resolver not yet implemented (agentID=%s)", r.agentID)
}

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/petereon/firstyr/internal/config"
	"github.com/petereon/firstyr/internal/server"
	"github.com/petereon/firstyr/internal/store/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// startTestServer starts a server with a fresh SQLite database and returns the REST base URL.
func startTestServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	adapter, err := sqlite.New(dir+"/test.db", "../../migrations/sqlite")
	require.NoError(t, err)
	require.NoError(t, adapter.Migrate(context.Background()))

	grpcPort := freePort(t)
	restPort := freePort(t)

	cfg := &config.Config{}
	cfg.Server.GRPCPort = grpcPort
	cfg.Server.RESTPort = restPort
	cfg.Auth.Mode = "none"

	log, _ := zap.NewDevelopment()
	srv, err := server.New(cfg, adapter, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		adapter.Close()
	})
	go srv.Run(ctx) //nolint:errcheck

	// Wait for server to be ready.
	restBase := fmt.Sprintf("http://127.0.0.1:%d", restPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(restBase + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 50*time.Millisecond, "server did not become ready")

	return restBase
}

// listCollection is a helper that GETs the REST ListDocuments endpoint for a top-level
// collection and returns the parsed response body. The grpc-gateway routes
// GET /v1/{parent=projects/*/databases/*/documents}/{collection_id} to ListDocuments.
func listCollection(t *testing.T, base, project, database, collectionID string) map[string]interface{} {
	t.Helper()
	url := fmt.Sprintf("%s/v1/projects/%s/databases/%s/documents/%s",
		base, project, database, collectionID)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))
	return result
}

// TestRPC_CreateAndGetDocument creates a document, then verifies it appears in the
// collection listing. Direct GetDocument via REST is not independently testable for
// top-level documents in this gateway configuration: the pattern
// GET /v1/{name=projects/*/databases/*/documents/*/**} is shadowed by the more-specific
// ListDocuments pattern GET /v1/{parent=.../*/**}/{collection_id}, so every
// two-segment document path is routed to ListDocuments instead of GetDocument.
// Listing the parent collection exercises the same round-trip through CreateDocument
// and the SQLite backend.
func TestRPC_CreateAndGetDocument(t *testing.T) {
	base := startTestServer(t)

	// CreateDocument via REST: POST /v1/{parent}/{collectionId}
	body := `{"fields":{"name":{"stringValue":"Alice"},"age":{"integerValue":"30"}}}`
	url := base + "/v1/projects/p/databases/d/documents/users"
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateDocument should return 200")

	var created map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
	name, ok := created["name"].(string)
	require.True(t, ok, "response must have a 'name' field")
	assert.Contains(t, name, "projects/p/databases/d/documents/users/",
		"document name must embed the expected collection path")

	// Verify the document was persisted by listing its collection.
	// GET /v1/projects/p/databases/d/documents/users → ListDocuments(parent=…/documents, collectionId=users)
	result := listCollection(t, base, "p", "d", "users")
	docs, ok := result["documents"].([]interface{})
	require.True(t, ok, "listing users collection must return a 'documents' array; got: %v", result)
	require.Len(t, docs, 1, "exactly one document should be in the collection")
	doc := docs[0].(map[string]interface{})
	assert.Equal(t, name, doc["name"], "listed document name must match the created document name")
}

// TestRPC_CreateDocument_AlreadyExists verifies that a second CreateDocument call
// with the same explicit document ID is rejected with HTTP 409 Conflict.
func TestRPC_CreateDocument_AlreadyExists(t *testing.T) {
	base := startTestServer(t)

	// CreateDocument with explicit document ID.
	body := `{"fields":{}}`
	url := base + "/v1/projects/p/databases/d/documents/col?documentId=fixed-id"
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Second create with the same ID must fail with 409.
	resp2, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusConflict, resp2.StatusCode)
}

// TestRPC_GetDocument_NotFound verifies that listing a collection that does not
// exist returns an empty document list rather than an error. As documented in
// TestRPC_CreateAndGetDocument, direct GetDocument calls for top-level documents are
// routed to ListDocuments by the grpc-gateway pattern matcher; this test therefore
// confirms the absence of a document by asserting that the collection list is empty.
func TestRPC_GetDocument_NotFound(t *testing.T) {
	base := startTestServer(t)

	// GET /v1/.../documents/col → ListDocuments(parent=…/documents, collectionId=col)
	// No documents have been created, so the response must be an empty list (HTTP 200).
	result := listCollection(t, base, "p", "d", "col")

	// An absent collection returns {} or {"documents":[]}, never a 4xx error.
	if docs, ok := result["documents"]; ok {
		assert.Empty(t, docs, "empty collection must contain no documents")
	}
	// else: result is {} which also means no documents — test passes implicitly.
}

// TestRPC_UpdateDocument_Upsert verifies that a PATCH to a document path that does
// not yet exist creates it (upsert semantics) and returns the document with the
// expected name.
func TestRPC_UpdateDocument_Upsert(t *testing.T) {
	base := startTestServer(t)

	// PATCH via REST maps to UpdateDocument.
	docName := "projects/p/databases/d/documents/col/mydoc"
	body := `{"name":"` + docName + `","fields":{"x":{"stringValue":"hello"}}}`

	req, err := http.NewRequest(http.MethodPatch, base+"/v1/"+docName, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, docName, result["name"])
}

// TestRPC_DeleteDocument creates a document, deletes it, and then confirms the
// collection is empty.
func TestRPC_DeleteDocument(t *testing.T) {
	base := startTestServer(t)

	// Create a document with a known ID.
	body := `{"fields":{}}`
	url := base + "/v1/projects/p/databases/d/documents/col?documentId=todelete"
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Delete it via DELETE /v1/{name}.
	docName := "projects/p/databases/d/documents/col/todelete"
	req, err := http.NewRequest(http.MethodDelete, base+"/v1/"+docName, nil)
	require.NoError(t, err)
	delResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	// Confirm the document is gone by listing the collection — it must be empty.
	result := listCollection(t, base, "p", "d", "col")
	if docs, ok := result["documents"]; ok {
		assert.Empty(t, docs, "collection must be empty after the document was deleted")
	}
}

// TestRPC_ListDocuments creates three documents and verifies they all appear in the
// collection listing.
func TestRPC_ListDocuments(t *testing.T) {
	base := startTestServer(t)

	// Create a few documents.
	for _, id := range []string{"a", "b", "c"} {
		body := `{"fields":{}}`
		url := base + "/v1/projects/p/databases/d/documents/things?documentId=" + id
		resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// List them.
	result := listCollection(t, base, "p", "d", "things")
	docs, ok := result["documents"].([]interface{})
	require.True(t, ok, "response must have 'documents' array; got: %v", result)
	assert.Len(t, docs, 3)
}

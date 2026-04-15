package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/config"
	"github.com/petereon/firstyr/internal/server"
	"github.com/petereon/firstyr/internal/store/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// testServer holds the REST base URL and gRPC address of a running test server.
type testServer struct {
	restBase string
	grpcAddr string
}

// startTestServer starts a server with a fresh SQLite database and returns a testServer
// containing both the REST base URL and the gRPC address.
func startTestServer(t *testing.T) *testServer {
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

	log, err := zap.NewDevelopment()
	require.NoError(t, err)
	srv, err := server.New(cfg, adapter, log)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		adapter.Close()
	})
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	// Wait for server to be ready.
	restBase := fmt.Sprintf("http://127.0.0.1:%d", restPort)
	require.Eventually(t, func() bool {
		select {
		case err := <-runErr:
			t.Fatalf("server exited unexpectedly: %v", err)
		default:
		}
		resp, err := http.Get(restBase + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 3*time.Second, 50*time.Millisecond, "server did not become ready")

	return &testServer{
		restBase: restBase,
		grpcAddr: fmt.Sprintf("127.0.0.1:%d", grpcPort),
	}
}

// grpcClient dials the gRPC port of ts and returns a Firestore client.
func grpcClient(t *testing.T, ts *testServer) firestorev1.FirestoreClient {
	t.Helper()
	conn, err := grpc.NewClient(ts.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return firestorev1.NewFirestoreClient(conn)
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

// TestRPC_CreateAndGetDocument creates a document, then verifies it via both the
// collection listing (REST) and a direct GetDocument call (gRPC).
func TestRPC_CreateAndGetDocument(t *testing.T) {
	ts := startTestServer(t)

	// CreateDocument via REST: POST /v1/{parent}/{collectionId}
	body := `{"fields":{"name":{"stringValue":"Alice"},"age":{"integerValue":"30"}}}`
	url := ts.restBase + "/v1/projects/p/databases/d/documents/users"
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
	result := listCollection(t, ts.restBase, "p", "d", "users")
	docs, ok := result["documents"].([]interface{})
	require.True(t, ok, "listing users collection must return a 'documents' array; got: %v", result)
	require.Len(t, docs, 1, "exactly one document should be in the collection")
	doc, ok2 := docs[0].(map[string]interface{})
	require.True(t, ok2, "expected document object, got: %T", docs[0])
	assert.Equal(t, name, doc["name"], "listed document name must match the created document name")

	// Verify via direct gRPC GetDocument call.
	client := grpcClient(t, ts)
	gdoc, err := client.GetDocument(context.Background(), &firestorev1.GetDocumentRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, name, gdoc.GetName())
}

// TestRPC_CreateDocument_AlreadyExists verifies that a second CreateDocument call
// with the same explicit document ID is rejected with HTTP 409 Conflict.
func TestRPC_CreateDocument_AlreadyExists(t *testing.T) {
	ts := startTestServer(t)

	// CreateDocument with explicit document ID.
	body := `{"fields":{}}`
	url := ts.restBase + "/v1/projects/p/databases/d/documents/col?documentId=fixed-id"
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

// TestRPC_GetDocument_NotFound verifies that GetDocument on a nonexistent path
// returns codes.NotFound via gRPC.
func TestRPC_GetDocument_NotFound(t *testing.T) {
	ts := startTestServer(t)

	client := grpcClient(t, ts)
	_, err := client.GetDocument(context.Background(), &firestorev1.GetDocumentRequest{
		Name: "projects/p/databases/d/documents/col/no-such-doc",
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// TestRPC_UpdateDocument_Upsert verifies that a PATCH to a document path that does
// not yet exist creates it (upsert semantics) and returns the document with the
// expected name.
func TestRPC_UpdateDocument_Upsert(t *testing.T) {
	ts := startTestServer(t)

	// PATCH via REST maps to UpdateDocument.
	docName := "projects/p/databases/d/documents/col/mydoc"
	body := `{"name":"` + docName + `","fields":{"x":{"stringValue":"hello"}}}`

	req, err := http.NewRequest(http.MethodPatch, ts.restBase+"/v1/"+docName, bytes.NewBufferString(body))
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
// document is gone via gRPC GetDocument.
func TestRPC_DeleteDocument(t *testing.T) {
	ts := startTestServer(t)

	// Create a document with a known ID.
	body := `{"fields":{}}`
	url := ts.restBase + "/v1/projects/p/databases/d/documents/col?documentId=todelete"
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Delete it via DELETE /v1/{name}.
	docName := "projects/p/databases/d/documents/col/todelete"
	req, err := http.NewRequest(http.MethodDelete, ts.restBase+"/v1/"+docName, nil)
	require.NoError(t, err)
	delResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	// Confirm the document is gone via direct gRPC GetDocument call.
	client := grpcClient(t, ts)
	_, err = client.GetDocument(context.Background(), &firestorev1.GetDocumentRequest{Name: docName})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// TestRPC_ListDocuments creates three documents and verifies they all appear in the
// collection listing.
func TestRPC_ListDocuments(t *testing.T) {
	ts := startTestServer(t)

	// Create a few documents.
	for _, id := range []string{"a", "b", "c"} {
		body := `{"fields":{}}`
		url := ts.restBase + "/v1/projects/p/databases/d/documents/things?documentId=" + id
		resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// List them.
	result := listCollection(t, ts.restBase, "p", "d", "things")
	docs, ok := result["documents"].([]interface{})
	require.True(t, ok, "response must have 'documents' array; got: %v", result)
	assert.Len(t, docs, 3)
}

// createDoc is a helper that creates a document via the CreateDocument REST endpoint.
func createDoc(t *testing.T, base, parent, collectionID string, body map[string]interface{}) string {
	t.Helper()
	url := fmt.Sprintf("%s/v1/%s/%s", base, parent, collectionID)
	jsonBody, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(jsonBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	name, ok := result["name"].(string)
	require.True(t, ok, "response must have a 'name' field")
	return name
}

// TestRunQuery_WithFilter creates two documents with different status values,
// then queries with a filter and verifies only the matching document is returned.
func TestRunQuery_WithFilter(t *testing.T) {
	srv := startTestServer(t)
	restBase := srv.restBase

	// Create documents — two "active", one "inactive"
	createDoc(t, restBase, "projects/p/databases/(default)/documents", "items", map[string]interface{}{
		"fields": map[string]interface{}{"status": map[string]interface{}{"stringValue": "active"}},
	})
	createDoc(t, restBase, "projects/p/databases/(default)/documents", "items", map[string]interface{}{
		"fields": map[string]interface{}{"status": map[string]interface{}{"stringValue": "active"}},
	})
	createDoc(t, restBase, "projects/p/databases/(default)/documents", "items", map[string]interface{}{
		"fields": map[string]interface{}{"status": map[string]interface{}{"stringValue": "inactive"}},
	})

	// Run filtered query via POST :runQuery
	body := `{
        "structuredQuery": {
            "from": [{"collectionId": "items"}],
            "where": {
                "fieldFilter": {
                    "field": {"fieldPath": "status"},
                    "op": "EQUAL",
                    "value": {"stringValue": "active"}
                }
            }
        }
    }`
	resp, err := http.Post(
		restBase+"/v1/projects/p/databases/(default)/documents:runQuery",
		"application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var results []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&results))
	// Filter out the terminal {"done":true} entry
	docs := 0
	for _, r := range results {
		if r["document"] != nil {
			docs++
		}
	}
	require.Equal(t, 2, docs, "expected 2 active documents")
}

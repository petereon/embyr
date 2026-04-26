package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// TestRunQuery_ErrorBeforeDocuments verifies that a :runQuery with an invalid
// request (no structured_query) returns a proper HTTP error, not a malformed JSON array.
func TestRunQuery_ErrorBeforeDocuments(t *testing.T) {
	srv := startTestServer(t)

	// Send a runQuery with an empty body (no structuredQuery) — should fail before any docs.
	resp, err := http.Post(
		srv.restBase+"/v1/projects/p/databases/(default)/documents:runQuery",
		"application/json",
		strings.NewReader("{}"),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Should be a 4xx, not 200 with a broken array.
	require.NotEqual(t, http.StatusOK, resp.StatusCode, "invalid runQuery must not return 200")
	body, _ := io.ReadAll(resp.Body)
	// Body must not start with "[" since no documents were written.
	assert.False(t, strings.HasPrefix(strings.TrimSpace(string(body)), "["),
		"error before first doc must not start a JSON array")
}

// TestCommit_ServerTimestamp verifies that serverTimestamp transforms work in Commit.
func TestCommit_ServerTimestamp(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/ts/doc1"
	resp, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name:   docPath,
					Fields: map[string]*firestorev1.Value{},
				},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "createdAt",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_SetToServerValue{
					SetToServerValue: firestorev1.DocumentTransform_FieldTransform_REQUEST_TIME,
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.WriteResults, 1)

	// Verify the field was set
	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	_, ok := doc.Fields["createdAt"]
	require.True(t, ok, "createdAt field should have been set by serverTimestamp")
	require.NotNil(t, doc.Fields["createdAt"].GetTimestampValue(), "createdAt should be a timestamp")
}

// TestFieldTransform_SetToServerValue_Unrecognized verifies that an unrecognized
// SetToServerValue (SERVER_VALUE_UNSPECIFIED = 0) produces a nil entry in
// TransformResults without panicking.
func TestFieldTransform_SetToServerValue_Unrecognized(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/ft/unrecognized"
	resp, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name:   docPath,
					Fields: map[string]*firestorev1.Value{},
				},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "x",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_SetToServerValue{
					SetToServerValue: 0, // SERVER_VALUE_UNSPECIFIED — not handled
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, resp.WriteResults, 1)
	// TransformResults has one entry but it is nil (unrecognized server value).
	results := resp.WriteResults[0].GetTransformResults()
	require.Len(t, results, 1)
	// The handler appends nil for an unrecognized SetToServerValue. gRPC wire
	// normalises nil *Value to an empty Value message, so the result is non-nil
	// but carries no value type.
	assert.Nil(t, results[0].GetValueType(), "unrecognized SetToServerValue must produce a Value with no type set")
}

// TestFieldTransform_Increment_NonNumericDelta verifies that Increment with a
// non-numeric delta (e.g. a string value) returns an error rather than silently
// writing nil to the document field.
func TestFieldTransform_Increment_NonNumericDelta(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/ft/inc-badtype"
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name:   docPath,
					Fields: map[string]*firestorev1.Value{},
				},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "counter",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_Increment{
					Increment: &firestorev1.Value{
						ValueType: &firestorev1.Value_StringValue{StringValue: "not-a-number"},
					},
				},
			}},
		}},
	})
	require.Error(t, err, "Increment with string delta must return an error")
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestFieldTransform_AppendMissingElements_NilCurr verifies AppendMissingElements
// when the target field does not yet exist — should create a fresh array.
func TestFieldTransform_AppendMissingElements_NilCurr(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/ft/append-nil"
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name:   docPath,
					Fields: map[string]*firestorev1.Value{},
				},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "tags",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_AppendMissingElements{
					AppendMissingElements: &firestorev1.ArrayValue{
						Values: []*firestorev1.Value{
							{ValueType: &firestorev1.Value_StringValue{StringValue: "a"}},
							{ValueType: &firestorev1.Value_StringValue{StringValue: "b"}},
						},
					},
				},
			}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	tags := doc.Fields["tags"].GetArrayValue().GetValues()
	require.Len(t, tags, 2)
	assert.Equal(t, "a", tags[0].GetStringValue())
	assert.Equal(t, "b", tags[1].GetStringValue())
}

// TestFieldTransform_AppendMissingElements_Dedup verifies that an element already
// in the array is not added again.
func TestFieldTransform_AppendMissingElements_Dedup(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/ft/append-dedup"
	sv := func(s string) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
	}

	// Create doc with tags=["a","b"].
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"tags": {ValueType: &firestorev1.Value_ArrayValue{ArrayValue: &firestorev1.ArrayValue{
							Values: []*firestorev1.Value{sv("a"), sv("b")},
						}}},
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Append ["b", "c"] — "b" already exists, only "c" should be added.
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{Name: docPath, Fields: map[string]*firestorev1.Value{}},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "tags",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_AppendMissingElements{
					AppendMissingElements: &firestorev1.ArrayValue{
						Values: []*firestorev1.Value{sv("b"), sv("c")},
					},
				},
			}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	tags := doc.Fields["tags"].GetArrayValue().GetValues()
	require.Len(t, tags, 3, "expected [a, b, c] — b must not be duplicated")
	assert.Equal(t, "a", tags[0].GetStringValue())
	assert.Equal(t, "b", tags[1].GetStringValue())
	assert.Equal(t, "c", tags[2].GetStringValue())
}

// TestFieldTransform_RemoveAllFromArray_NoMatch verifies that RemoveAllFromArray
// leaves the array unchanged when none of the elements match.
func TestFieldTransform_RemoveAllFromArray_NoMatch(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/ft/remove-nomatch"
	sv := func(s string) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
	}

	// Create doc with tags=["x","y"].
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"tags": {ValueType: &firestorev1.Value_ArrayValue{ArrayValue: &firestorev1.ArrayValue{
							Values: []*firestorev1.Value{sv("x"), sv("y")},
						}}},
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Remove ["z"] — not in the array; tags should remain ["x","y"].
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{Name: docPath, Fields: map[string]*firestorev1.Value{}},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "tags",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_RemoveAllFromArray{
					RemoveAllFromArray: &firestorev1.ArrayValue{
						Values: []*firestorev1.Value{sv("z")},
					},
				},
			}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	tags := doc.Fields["tags"].GetArrayValue().GetValues()
	require.Len(t, tags, 2, "array must be unchanged when no elements matched")
	assert.Equal(t, "x", tags[0].GetStringValue())
	assert.Equal(t, "y", tags[1].GetStringValue())
}

// TestUpdateDoc_MaskPreservesUnmaskedFields verifies that updateDoc with an
// update_mask only modifies the masked fields and leaves all others intact.
// Regression test for the bug where WriteModeUpdate replaced the entire document.
func TestUpdateDoc_MaskPreservesUnmaskedFields(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/mask/doc1"
	iv := func(n int64) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: n}}
	}
	sv := func(s string) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
	}

	// Create document with text, category, votes.
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"text":     sv("hello"),
						"category": sv("idea"),
						"votes":    iv(0),
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Update only votes via update_mask + increment transform (simulates updateDoc increment).
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{Name: docPath, Fields: map[string]*firestorev1.Value{}},
			},
			UpdateMask: &firestorev1.DocumentMask{FieldPaths: []string{"votes"}},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "votes",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_Increment{
					Increment: iv(1),
				},
			}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	assert.Equal(t, "hello", doc.Fields["text"].GetStringValue(), "text must be preserved")
	assert.Equal(t, "idea", doc.Fields["category"].GetStringValue(), "category must be preserved")
	assert.Equal(t, int64(1), doc.Fields["votes"].GetIntegerValue(), "votes must be incremented")
}

// TestUpdateDoc_MaskDeletesRemovedField verifies that a field in the mask
// but absent from the update payload is deleted from the document.
func TestUpdateDoc_MaskDeletesRemovedField(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/mask/doc2"
	sv := func(s string) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
	}

	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name:   docPath,
					Fields: map[string]*firestorev1.Value{"a": sv("keep"), "b": sv("delete me")},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Mask includes "b" but update fields don't — "b" should be deleted, "a" preserved.
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{Name: docPath, Fields: map[string]*firestorev1.Value{}},
			},
			UpdateMask: &firestorev1.DocumentMask{FieldPaths: []string{"b"}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	assert.Equal(t, "keep", doc.Fields["a"].GetStringValue(), "unmasked field must be preserved")
	_, hasB := doc.Fields["b"]
	assert.False(t, hasB, "masked absent field must be deleted")
}

// TestUpdateDoc_NestedMask_PreservesUntouched verifies that a dotted field-mask
// path (e.g. "profile.age") replaces only that leaf and leaves sibling fields intact.
func TestUpdateDoc_NestedMask_PreservesUntouched(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/nested/doc1"
	iv := func(n int64) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: n}}
	}
	sv := func(s string) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
	}
	mv := func(fields map[string]*firestorev1.Value) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_MapValue{
			MapValue: &firestorev1.MapValue{Fields: fields},
		}}
	}

	// Create: {profile: {name: "alice", age: 30}}
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"profile": mv(map[string]*firestorev1.Value{
							"name": sv("alice"),
							"age":  iv(30),
						}),
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Update only profile.age via nested mask.
	// Incoming document carries {profile: {age: 31}}.
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"profile": mv(map[string]*firestorev1.Value{
							"age": iv(31),
						}),
					},
				},
			},
			UpdateMask: &firestorev1.DocumentMask{FieldPaths: []string{"profile.age"}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)

	profile := doc.Fields["profile"].GetMapValue().GetFields()
	require.NotNil(t, profile, "profile map must exist")
	assert.Equal(t, int64(31), profile["age"].GetIntegerValue(), "profile.age must be updated")
	assert.Equal(t, "alice", profile["name"].GetStringValue(), "profile.name must be preserved")
}

// TestUpdateDoc_NestedMask_DeletesLeafField verifies that a dotted mask path
// absent from the incoming document deletes only that leaf, leaving siblings intact.
func TestUpdateDoc_NestedMask_DeletesLeafField(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/nested/doc2"
	sv := func(s string) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
	}
	mv := func(fields map[string]*firestorev1.Value) *firestorev1.Value {
		return &firestorev1.Value{ValueType: &firestorev1.Value_MapValue{
			MapValue: &firestorev1.MapValue{Fields: fields},
		}}
	}

	// Create: {address: {street: "Main St", city: "Springfield"}}
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"address": mv(map[string]*firestorev1.Value{
							"street": sv("Main St"),
							"city":   sv("Springfield"),
						}),
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Mask includes address.city but incoming has no address.city — delete only city.
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name:   docPath,
					Fields: map[string]*firestorev1.Value{},
				},
			},
			UpdateMask: &firestorev1.DocumentMask{FieldPaths: []string{"address.city"}},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)

	address := doc.Fields["address"].GetMapValue().GetFields()
	require.NotNil(t, address, "address map must still exist")
	assert.Equal(t, "Main St", address["street"].GetStringValue(), "address.street must be preserved")
	_, hasCity := address["city"]
	assert.False(t, hasCity, "address.city must be deleted by mask")
}

func TestBeginRollback(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	beginResp, err := client.BeginTransaction(ctx, &firestorev1.BeginTransactionRequest{
		Database: "projects/p/databases/(default)",
	})
	require.NoError(t, err)
	require.NotEmpty(t, beginResp.GetTransaction())

	_, err = client.Rollback(ctx, &firestorev1.RollbackRequest{
		Database:    "projects/p/databases/(default)",
		Transaction: beginResp.GetTransaction(),
	})
	require.NoError(t, err)
}

// TestCommit_Transaction_VerifyMutation verifies that a VerifyMutation write
// (nil operation, precondition only) inside a transaction produces exactly one
// WriteResult in the CommitResponse so client-side index alignment is correct.
func TestCommit_Transaction_VerifyMutation(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	const db = "projects/p/databases/(default)"
	docPath := db + "/documents/verify/doc1"

	// Create a document so the precondition "exists" can be satisfied.
	_, err := client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       db + "/documents",
		CollectionId: "verify",
		DocumentId:   "doc1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}},
			},
		},
	})
	require.NoError(t, err)

	beginResp, err := client.BeginTransaction(ctx, &firestorev1.BeginTransactionRequest{Database: db})
	require.NoError(t, err)

	commitResp, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database:    db,
		Transaction: beginResp.GetTransaction(),
		Writes: []*firestorev1.Write{
			// Write 1: VerifyMutation — nil operation, precondition only.
			{
				CurrentDocument: &firestorev1.Precondition{
					ConditionType: &firestorev1.Precondition_Exists{Exists: true},
				},
			},
			// Write 2: real update.
			{
				Operation: &firestorev1.Write_Update{
					Update: &firestorev1.Document{
						Name: docPath,
						Fields: map[string]*firestorev1.Value{
							"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 2}},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	// Firestore spec: one WriteResult per Write. Two writes → two results.
	require.Len(t, commitResp.GetWriteResults(), 2, "expected one WriteResult per Write")
}

// TestBatchGetDocuments_REST_JSONArray verifies that POST …:batchGet returns
// a JSON array, not NDJSON. The Firebase SDK calls .forEach() on the full
// response body, so it must be parseable as a JSON array.
func TestBatchGetDocuments_REST_JSONArray(t *testing.T) {
	srv := startTestServer(t)

	// Create one document; request it plus a missing one.
	docA := "projects/p/databases/(default)/documents/bg/a"
	createDoc(t, srv.restBase, "projects/p/databases/(default)/documents", "bg", map[string]interface{}{
		"fields": map[string]interface{}{"x": map[string]interface{}{"stringValue": "hello"}},
	})
	// Use the exact name returned from createDoc for the assertion
	result := listCollection(t, srv.restBase, "p", "(default)", "bg")
	docs := result["documents"].([]interface{})
	require.Len(t, docs, 1)
	docA = docs[0].(map[string]interface{})["name"].(string)

	docMissing := "projects/p/databases/(default)/documents/bg/ghost"
	body := fmt.Sprintf(`{"documents":[%q,%q]}`, docA, docMissing)
	resp, err := http.Post(
		srv.restBase+"/v1/projects/p/databases/(default)/documents:batchGet",
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Must be a JSON array (not NDJSON) for the Firebase SDK .forEach() call.
	var arr []map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &arr), "response must be a JSON array, got: %s", string(raw))
	require.Len(t, arr, 2)

	var found, missing int
	for _, item := range arr {
		if _, ok := item["found"]; ok {
			found++
		}
		if _, ok := item["missing"]; ok {
			missing++
		}
	}
	assert.Equal(t, 1, found, "expected 1 found document")
	assert.Equal(t, 1, missing, "expected 1 missing document")
}

// TestBatchGetDocuments_FoundAndMissing creates two documents, then calls
// BatchGetDocuments asking for those two plus a nonexistent document.
// It verifies that found docs are returned as Found results and the missing
// doc is returned as a Missing result.
func TestBatchGetDocuments_FoundAndMissing(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	// Create two documents.
	docA := "projects/p/databases/(default)/documents/batch/a"
	docB := "projects/p/databases/(default)/documents/batch/b"
	docMissing := "projects/p/databases/(default)/documents/batch/ghost"

	for _, path := range []string{docA, docB} {
		_, err := client.Commit(ctx, &firestorev1.CommitRequest{
			Database: "projects/p/databases/(default)",
			Writes: []*firestorev1.Write{{
				Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
					Name:   path,
					Fields: map[string]*firestorev1.Value{"v": {ValueType: &firestorev1.Value_StringValue{StringValue: path}}},
				}},
			}},
		})
		require.NoError(t, err)
	}

	stream, err := client.BatchGetDocuments(ctx, &firestorev1.BatchGetDocumentsRequest{
		Database:  "projects/p/databases/(default)",
		Documents: []string{docA, docB, docMissing},
	})
	require.NoError(t, err)

	var found, missing []string
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		switch r := resp.Result.(type) {
		case *firestorev1.BatchGetDocumentsResponse_Found:
			found = append(found, r.Found.GetName())
		case *firestorev1.BatchGetDocumentsResponse_Missing:
			missing = append(missing, r.Missing)
		}
	}

	assert.ElementsMatch(t, []string{docA, docB}, found)
	assert.ElementsMatch(t, []string{docMissing}, missing)
}

// TestBatchGetDocuments_NewTransaction verifies the new_transaction flow used
// by runTransaction in the Firebase SDK: the first response must carry the
// transaction ID with no document result, and subsequent responses carry found
// or missing document results.
func TestBatchGetDocuments_NewTransaction(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	docPath := "projects/p/databases/(default)/documents/txcol/doc1"
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name:   docPath,
				Fields: map[string]*firestorev1.Value{"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 42}}},
			}},
		}},
	})
	require.NoError(t, err)

	stream, err := client.BatchGetDocuments(ctx, &firestorev1.BatchGetDocumentsRequest{
		Database:  "projects/p/databases/(default)",
		Documents: []string{docPath},
		ConsistencySelector: &firestorev1.BatchGetDocumentsRequest_NewTransaction{
			NewTransaction: &firestorev1.TransactionOptions{},
		},
	})
	require.NoError(t, err)

	// First response must be the transaction ID with no result.
	first, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, first.GetTransaction(), "first response must contain a transaction ID")
	assert.Nil(t, first.Result, "first response must have no document result")

	txID := first.GetTransaction()

	// Remaining responses carry documents.
	var found []string
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if f, ok := resp.Result.(*firestorev1.BatchGetDocumentsResponse_Found); ok {
			found = append(found, f.Found.GetName())
		}
	}
	assert.ElementsMatch(t, []string{docPath}, found)

	// The transaction must be usable — Commit with the txID must succeed.
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database:    "projects/p/databases/(default)",
		Transaction: txID,
	})
	require.NoError(t, err)
}

// TestCommit_Precondition_UpdateTime_Mismatch verifies that a Commit write with
// a currentDocument.updateTime precondition fails with FailedPrecondition when
// the precondition timestamp does not match the stored document's updateTime.
func TestCommit_Precondition_UpdateTime_Mismatch(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	const db = "projects/p/databases/(default)"
	docPath := db + "/documents/occ/doc1"

	// Create the document.
	_, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: db,
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}},
					},
				},
			},
		}},
	})
	require.NoError(t, err)

	// Try to update with a clearly wrong updateTime (epoch).
	wrongTime := timestamppb.New(time.Time{})
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: db,
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 2}},
					},
				},
			},
			CurrentDocument: &firestorev1.Precondition{
				ConditionType: &firestorev1.Precondition_UpdateTime{UpdateTime: wrongTime},
			},
		}},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "wrong updateTime must yield FailedPrecondition")

	// Verify document is unchanged.
	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	assert.Equal(t, int64(1), doc.Fields["x"].GetIntegerValue(), "document must not be modified")
}

// TestCommit_Precondition_UpdateTime_Match verifies that a Commit write with
// a matching currentDocument.updateTime precondition succeeds.
func TestCommit_Precondition_UpdateTime_Match(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	const db = "projects/p/databases/(default)"
	docPath := db + "/documents/occ/doc2"

	// Create the document.
	commitResp, err := client.Commit(ctx, &firestorev1.CommitRequest{
		Database: db,
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}},
					},
				},
			},
		}},
	})
	require.NoError(t, err)
	updateTime := commitResp.GetWriteResults()[0].GetUpdateTime()

	// Update with the correct updateTime.
	_, err = client.Commit(ctx, &firestorev1.CommitRequest{
		Database: db,
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 2}},
					},
				},
			},
			CurrentDocument: &firestorev1.Precondition{
				ConditionType: &firestorev1.Precondition_UpdateTime{UpdateTime: updateTime},
			},
		}},
	})
	require.NoError(t, err)

	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	assert.Equal(t, int64(2), doc.Fields["x"].GetIntegerValue(), "document must be updated")
}

func TestBatchWrite(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	resp, err := client.BatchWrite(ctx, &firestorev1.BatchWriteRequest{
		Database: "projects/p/databases/(default)",
		Writes: []*firestorev1.Write{
			{Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name:   "projects/p/databases/(default)/documents/batch/a",
				Fields: map[string]*firestorev1.Value{"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 1}}},
			}}},
			{Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name:   "projects/p/databases/(default)/documents/batch/b",
				Fields: map[string]*firestorev1.Value{"x": {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 2}}},
			}}},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetWriteResults(), 2)
}

// TestWrite_Stream verifies the Write bidirectional stream: handshake → write batch → write result.
func TestListen_WithOrderBy(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := grpcClient(t, srv)

	const db = "projects/p/databases/(default)"
	const parent = db + "/documents"

	// Pre-create a document with a createdAt field.
	_, err := client.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: "listen_notes",
		DocumentId:   "doc1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"text":     {ValueType: &firestorev1.Value_StringValue{StringValue: "hello"}},
				"category": {ValueType: &firestorev1.Value_StringValue{StringValue: "general"}},
			},
		},
	})
	require.NoError(t, err)

	// Open Listen stream with orderBy — mirrors what the Firebase SDK sends for onSnapshot.
	stream, err := client.Listen(ctx)
	require.NoError(t, err)

	err = stream.Send(&firestorev1.ListenRequest{
		Database: db,
		TargetChange: &firestorev1.ListenRequest_AddTarget{
			AddTarget: &firestorev1.Target{
				TargetId: 2,
				TargetType: &firestorev1.Target_Query{
					Query: &firestorev1.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorev1.StructuredQuery{
								From: []*firestorev1.StructuredQuery_CollectionSelector{
									{CollectionId: "listen_notes"},
								},
								OrderBy: []*firestorev1.StructuredQuery_Order{
									{
										Field:     &firestorev1.StructuredQuery_FieldReference{FieldPath: "text"},
										Direction: firestorev1.StructuredQuery_DESCENDING,
									},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Must receive ADD, a DocumentChange, and CURRENT.
	gotDoc := false
	gotCurrent := false
	for i := 0; i < 5; i++ {
		resp, err := stream.Recv()
		require.NoError(t, err, "Listen stream terminated unexpectedly")
		switch r := resp.GetResponseType().(type) {
		case *firestorev1.ListenResponse_DocumentChange:
			gotDoc = true
			assert.Equal(t, "doc1", lastSegment(r.DocumentChange.GetDocument().GetName()))
		case *firestorev1.ListenResponse_TargetChange:
			if r.TargetChange.GetTargetChangeType() == firestorev1.TargetChange_CURRENT {
				gotCurrent = true
			}
		}
		if gotDoc && gotCurrent {
			break
		}
	}
	assert.True(t, gotDoc, "should have received the pre-existing document")
	assert.True(t, gotCurrent, "should have received CURRENT after snapshot")
}

func lastSegment(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func TestWrite_Stream(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)

	stream, err := client.Write(ctx)
	require.NoError(t, err)

	// Send handshake (empty writes).
	err = stream.Send(&firestorev1.WriteRequest{
		Database: "projects/p/databases/(default)",
	})
	require.NoError(t, err)

	// Receive handshake response: must have stream_id, stream_token, no write_results.
	handshake, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, handshake.GetStreamId(), "handshake must return a stream_id")
	require.NotEmpty(t, handshake.GetStreamToken(), "handshake must return a stream_token")
	assert.Empty(t, handshake.GetWriteResults(), "handshake response must have no write results")

	// Send a write batch.
	docPath := "projects/p/databases/(default)/documents/wstream/doc1"
	err = stream.Send(&firestorev1.WriteRequest{
		StreamId:    handshake.GetStreamId(),
		StreamToken: handshake.GetStreamToken(),
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{Update: &firestorev1.Document{
				Name:   docPath,
				Fields: map[string]*firestorev1.Value{"v": {ValueType: &firestorev1.Value_StringValue{StringValue: "hello"}}},
			}},
		}},
	})
	require.NoError(t, err)

	// Receive write result.
	result, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, result.GetWriteResults(), 1, "should get one WriteResult")
	assert.NotNil(t, result.GetWriteResults()[0].GetUpdateTime(), "WriteResult must have update_time")

	// Close the stream.
	require.NoError(t, stream.CloseSend())

	// Verify the document was actually persisted.
	doc, err := client.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	assert.Equal(t, docPath, doc.GetName())
}

// ── BrowserChannel HTTP helpers ────────────────────────────────────────────────
//
// The Firebase SDK speaks BrowserChannel: proto frames travel as base64 in
// URL-encoded form POSTs, and server responses stream back as length-prefixed
// JSON chunks over a long-lived GET.  These helpers implement that exact wire
// format so we can drive the server from plain Go HTTP code.

// bcFormBody encodes a proto message as a BrowserChannel forward-channel form
// body using proto3-JSON (sendRawJson:true format):
// count=1&ofs=0&req0___data__=<proto3-json>
func bcFormBody(msg proto.Message) string {
	b, _ := protojson.Marshal(msg)
	return url.Values{
		"count":         {"1"},
		"ofs":           {"0"},
		"req0___data__": {string(b)},
	}.Encode()
}

// bcParseSessionID extracts the session ID from a BrowserChannel connect-chunk body.
// Format: <len>\n[[0,["c","<SID>","",8,8,0]],[1,["noop"]]]
func bcParseSessionID(t *testing.T, body string) string {
	t.Helper()
	nl := strings.IndexByte(body, '\n')
	require.GreaterOrEqual(t, nl, 0, "connect chunk must contain newline, got: %q", body)
	var outer []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body[nl+1:]), &outer))
	require.GreaterOrEqual(t, len(outer), 1)
	var msg0 []json.RawMessage
	require.NoError(t, json.Unmarshal(outer[0], &msg0))
	require.GreaterOrEqual(t, len(msg0), 2)
	var inner []json.RawMessage
	require.NoError(t, json.Unmarshal(msg0[1], &inner))
	require.GreaterOrEqual(t, len(inner), 2)
	var msgType, sid string
	require.NoError(t, json.Unmarshal(inner[0], &msgType))
	require.Equal(t, "c", msgType, "expected 'c' control message")
	require.NoError(t, json.Unmarshal(inner[1], &sid))
	return sid
}

// bcChunk holds one decoded BrowserChannel back-channel chunk.
type bcChunk struct {
	raw json.RawMessage // proto3-JSON object (nil for noop chunks)
	err error
}

// bcReadChunks reads BrowserChannel data chunks from r and sends decoded proto
// bytes to ch.  Noop keep-alive chunks are skipped silently.  Stops after
// reading maxChunks data (non-noop) chunks or on EOF/error.
// Must be called from a goroutine; does NOT call t methods.
func bcReadChunks(r io.Reader, maxChunks int, ch chan<- bcChunk) {
	br := bufio.NewReaderSize(r, 64*1024)
	sent := 0
	for sent < maxChunks {
		// Each BrowserChannel chunk: "<N>\n<N bytes of JSON>"
		lenLine, err := br.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				ch <- bcChunk{err: err}
			}
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(lenLine))
		if err != nil {
			ch <- bcChunk{err: fmt.Errorf("bcReadChunks: parse length %q: %w", lenLine, err)}
			return
		}
		jsonBuf := make([]byte, n)
		if _, err := io.ReadFull(br, jsonBuf); err != nil {
			ch <- bcChunk{err: fmt.Errorf("bcReadChunks: read json body: %w", err)}
			return
		}
		// Parse [[seq, ["<base64>"]]] or [[seq, ["noop"]]]
		var outer []json.RawMessage
		if err := json.Unmarshal(jsonBuf, &outer); err != nil || len(outer) == 0 {
			continue // malformed; skip
		}
		var entry []json.RawMessage
		if err := json.Unmarshal(outer[0], &entry); err != nil || len(entry) < 2 {
			continue
		}
		var payload []json.RawMessage
		if err := json.Unmarshal(entry[1], &payload); err != nil || len(payload) == 0 {
			continue
		}
		// payload[0] is either "noop" (JSON string) or a proto3-JSON object.
		var noop string
		if json.Unmarshal(payload[0], &noop) == nil {
			continue // string value — noop or other control; skip
		}
		ch <- bcChunk{raw: json.RawMessage(payload[0])}
		sent++
	}
}

// TestBrowserChannel_Write_AddDocWithServerTimestamp exercises the complete
// BrowserChannel Write protocol at the HTTP layer — exactly what the Firebase
// JS SDK sends when addDoc(..., { createdAt: serverTimestamp() }) is called.
//
//  1. POST (no SID) → establish session, get stream_id / stream_token
//  2. GET (with SID) → open back channel to receive WriteResponses
//  3. POST (with SID) → send Write with update_transforms[serverTimestamp]
//  4. Assert WriteResult arrives via back channel
//  5. Assert document persisted with createdAt timestamp field
func TestBrowserChannel_Write_AddDocWithServerTimestamp(t *testing.T) {
	ts := startTestServer(t)
	const db     = "projects/p/databases/(default)"
	const parent = db + "/documents"
	writeURL := ts.restBase + "/google.firestore.v1.Firestore/Write/channel"

	// ── Step 1: Establish session ──────────────────────────────────────────────
	postResp, err := http.Post(
		writeURL+"?VER=8&RID=rpc",
		"application/x-www-form-urlencoded",
		strings.NewReader(bcFormBody(&firestorev1.WriteRequest{Database: db})),
	)
	require.NoError(t, err)
	defer postResp.Body.Close()
	require.Equal(t, http.StatusOK, postResp.StatusCode)
	rawBody, err := io.ReadAll(postResp.Body)
	require.NoError(t, err)
	sid := bcParseSessionID(t, string(rawBody))
	require.NotEmpty(t, sid, "session ID must not be empty after connect-chunk")
	t.Logf("BrowserChannel Write session: SID=%s", sid)

	// ── Step 2: Open GET back channel ─────────────────────────────────────────
	// Read 2 data chunks: handshake WriteResponse then the write WriteResponse.
	chunks := make(chan bcChunk, 4)
	go func() {
		getResp, err := http.Get(writeURL + "?VER=8&SID=" + sid + "&RID=rpc&TYPE=xmlhttp")
		if err != nil {
			chunks <- bcChunk{err: err}
			return
		}
		defer getResp.Body.Close()
		bcReadChunks(getResp.Body, 2, chunks)
	}()

	// ── Step 3: Read handshake WriteResponse ──────────────────────────────────
	var hsResp firestorev1.WriteResponse
	select {
	case c := <-chunks:
		require.NoError(t, c.err, "back-channel error reading handshake")
		require.NoError(t, protojson.Unmarshal(c.raw, &hsResp))
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for handshake WriteResponse")
	}
	require.NotEmpty(t, hsResp.GetStreamId(), "handshake must return stream_id")
	require.NotEmpty(t, hsResp.GetStreamToken(), "handshake must return stream_token")
	t.Logf("Handshake WriteResponse: stream_id=%s", hsResp.GetStreamId())

	// ── Step 4: Send write with serverTimestamp() ──────────────────────────────
	docPath := parent + "/notes/testdoc1"
	wr2, err := http.Post(
		writeURL+"?VER=8&SID="+sid+"&RID=2",
		"application/x-www-form-urlencoded",
		strings.NewReader(bcFormBody(&firestorev1.WriteRequest{
			Database:    db,
			StreamId:    hsResp.GetStreamId(),
			StreamToken: hsResp.GetStreamToken(),
			Writes: []*firestorev1.Write{{
				Operation: &firestorev1.Write_Update{
					Update: &firestorev1.Document{
						Name: docPath,
						Fields: map[string]*firestorev1.Value{
							"text":     {ValueType: &firestorev1.Value_StringValue{StringValue: "hello"}},
							"votes":    {ValueType: &firestorev1.Value_IntegerValue{IntegerValue: 0}},
							"tags":     {ValueType: &firestorev1.Value_ArrayValue{ArrayValue: &firestorev1.ArrayValue{}}},
							"category": {ValueType: &firestorev1.Value_StringValue{StringValue: "general"}},
						},
					},
				},
				UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
					FieldPath: "createdAt",
					TransformType: &firestorev1.DocumentTransform_FieldTransform_SetToServerValue{
						SetToServerValue: firestorev1.DocumentTransform_FieldTransform_REQUEST_TIME,
					},
				}},
			}},
		})),
	)
	require.NoError(t, err)
	wr2.Body.Close()
	require.Equal(t, http.StatusOK, wr2.StatusCode, "write POST must return 200")

	// ── Step 5: Assert WriteResult arrives via back channel ────────────────────
	var writeResp firestorev1.WriteResponse
	select {
	case c := <-chunks:
		require.NoError(t, c.err, "back-channel error reading write result")
		require.NoError(t, protojson.Unmarshal(c.raw, &writeResp))
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for WriteResult via back channel")
	}
	require.Len(t, writeResp.GetWriteResults(), 1, "expected one WriteResult")
	assert.NotNil(t, writeResp.GetWriteResults()[0].GetUpdateTime(), "WriteResult must have update_time")
	t.Logf("WriteResult received: update_time=%v", writeResp.GetWriteResults()[0].GetUpdateTime())

	// ── Step 6: Assert document persisted with createdAt timestamp field ───────
	client := grpcClient(t, ts)
	doc, err := client.GetDocument(context.Background(),
		&firestorev1.GetDocumentRequest{Name: docPath})
	require.NoError(t, err)
	createdAt, ok := doc.GetFields()["createdAt"]
	require.True(t, ok, "createdAt must be present in persisted document")
	assert.NotNil(t, createdAt.GetTimestampValue(), "createdAt must be a Timestamp value")
}

// TestBrowserChannel_Listen_ReceivesChangeAfterWrite checks the end-to-end
// flow that matches the browser app: establish a Listen channel with
// orderBy('createdAt','desc'), receive the initial (empty) snapshot, then
// write a doc via gRPC Write stream and confirm the DocumentChange arrives on
// the Listen back channel.
func TestBrowserChannel_Listen_ReceivesChangeAfterWrite(t *testing.T) {
	ts := startTestServer(t)
	const db     = "projects/p/databases/(default)"
	const parent = db + "/documents"
	listenURL := ts.restBase + "/google.firestore.v1.Firestore/Listen/channel"

	// ── Step 1: Establish Listen session (empty initial POST) ─────────────────
	postResp, err := http.Post(
		listenURL+"?VER=8&RID=rpc",
		"application/x-www-form-urlencoded",
		strings.NewReader("count=0&ofs=0"),
	)
	require.NoError(t, err)
	defer postResp.Body.Close()
	require.Equal(t, http.StatusOK, postResp.StatusCode)
	raw, err := io.ReadAll(postResp.Body)
	require.NoError(t, err)
	sid := bcParseSessionID(t, string(raw))
	require.NotEmpty(t, sid)
	t.Logf("Listen session established: SID=%s", sid)

	// ── Step 2: Open GET back channel ─────────────────────────────────────────
	// We expect: ADD TargetChange, CURRENT TargetChange, then one DocumentChange
	// after the write.  Read up to 5 chunks to find them all.
	listenChunks := make(chan bcChunk, 8)
	go func() {
		getResp, err := http.Get(listenURL + "?VER=8&SID=" + sid + "&RID=rpc&TYPE=xmlhttp")
		if err != nil {
			listenChunks <- bcChunk{err: err}
			return
		}
		defer getResp.Body.Close()
		bcReadChunks(getResp.Body, 5, listenChunks)
	}()

	// ── Step 3: Send AddTarget with orderBy('createdAt', 'desc') ──────────────
	addResp, err := http.Post(
		listenURL+"?VER=8&SID="+sid+"&RID=2",
		"application/x-www-form-urlencoded",
		strings.NewReader(bcFormBody(&firestorev1.ListenRequest{
			Database: db,
			TargetChange: &firestorev1.ListenRequest_AddTarget{
				AddTarget: &firestorev1.Target{
					TargetId: 2,
					TargetType: &firestorev1.Target_Query{
						Query: &firestorev1.Target_QueryTarget{
							Parent: parent,
							QueryType: &firestorev1.Target_QueryTarget_StructuredQuery{
								StructuredQuery: &firestorev1.StructuredQuery{
									From: []*firestorev1.StructuredQuery_CollectionSelector{
										{CollectionId: "notes"},
									},
									OrderBy: []*firestorev1.StructuredQuery_Order{
										{
											Field:     &firestorev1.StructuredQuery_FieldReference{FieldPath: "createdAt"},
											Direction: firestorev1.StructuredQuery_DESCENDING,
										},
										{
											Field:     &firestorev1.StructuredQuery_FieldReference{FieldPath: "__name__"},
											Direction: firestorev1.StructuredQuery_DESCENDING,
										},
									},
								},
							},
						},
					},
				},
			},
		})),
	)
	require.NoError(t, err)
	addResp.Body.Close()
	require.Equal(t, http.StatusOK, addResp.StatusCode)

	// ── Step 4: Drain initial snapshot (ADD + CURRENT target changes) ─────────
	gotAdd, gotCurrent := false, false
	deadline := time.After(3 * time.Second)
	for !gotAdd || !gotCurrent {
		select {
		case c := <-listenChunks:
			require.NoError(t, c.err, "Listen back-channel error during snapshot")
			var lr firestorev1.ListenResponse
			require.NoError(t, protojson.Unmarshal(c.raw, &lr))
			if tc, ok := lr.GetResponseType().(*firestorev1.ListenResponse_TargetChange); ok {
				switch tc.TargetChange.GetTargetChangeType() {
				case firestorev1.TargetChange_ADD:
					gotAdd = true
					t.Log("Got ADD TargetChange")
				case firestorev1.TargetChange_CURRENT:
					gotCurrent = true
					t.Log("Got CURRENT TargetChange (snapshot complete)")
				}
			}
		case <-deadline:
			t.Fatalf("timeout waiting for initial snapshot (ADD=%v CURRENT=%v)", gotAdd, gotCurrent)
		}
	}

	// ── Step 5: Write a doc with serverTimestamp via gRPC Write stream ─────────
	grpcStreamClient := grpcClient(t, ts)
	wstream, err := grpcStreamClient.Write(context.Background())
	require.NoError(t, err)
	require.NoError(t, wstream.Send(&firestorev1.WriteRequest{Database: db}))
	hsResp, err := wstream.Recv()
	require.NoError(t, err)

	docPath := parent + "/notes/live_doc1"
	require.NoError(t, wstream.Send(&firestorev1.WriteRequest{
		Database:    db,
		StreamId:    hsResp.GetStreamId(),
		StreamToken: hsResp.GetStreamToken(),
		Writes: []*firestorev1.Write{{
			Operation: &firestorev1.Write_Update{
				Update: &firestorev1.Document{
					Name: docPath,
					Fields: map[string]*firestorev1.Value{
						"text": {ValueType: &firestorev1.Value_StringValue{StringValue: "live update"}},
					},
				},
			},
			UpdateTransforms: []*firestorev1.DocumentTransform_FieldTransform{{
				FieldPath: "createdAt",
				TransformType: &firestorev1.DocumentTransform_FieldTransform_SetToServerValue{
					SetToServerValue: firestorev1.DocumentTransform_FieldTransform_REQUEST_TIME,
				},
			}},
		}},
	}))
	_, err = wstream.Recv() // consume WriteResult
	require.NoError(t, err)

	// ── Step 6: Expect DocumentChange to arrive on the Listen back channel ─────
	deadline2 := time.After(3 * time.Second)
	for {
		select {
		case c := <-listenChunks:
			require.NoError(t, c.err)
			var lr firestorev1.ListenResponse
			require.NoError(t, protojson.Unmarshal(c.raw, &lr))
			if dc, ok := lr.GetResponseType().(*firestorev1.ListenResponse_DocumentChange); ok {
				name := dc.DocumentChange.GetDocument().GetName()
				assert.Equal(t, docPath, name, "DocumentChange must be for the written doc")
				fields := dc.DocumentChange.GetDocument().GetFields()
				assert.NotNil(t, fields["createdAt"].GetTimestampValue(),
					"DocumentChange doc must include the serverTimestamp createdAt")
				t.Logf("DocumentChange received for %s with createdAt=%v",
					name, fields["createdAt"].GetTimestampValue())
				return // success
			}
		case <-deadline2:
			t.Fatal("timeout: DocumentChange for written doc never arrived on Listen back channel")
		}
	}
}

// TestRunAggregationQuery_Count creates three documents and verifies that
// RunAggregationQuery returns a COUNT of 3 under the requested alias.
func TestRunAggregationQuery_Count(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)
	restBase := srv.restBase

	const db = "projects/p/databases/(default)"
	const parent = db + "/documents"

	for i := 0; i < 3; i++ {
		createDoc(t, restBase, parent, "agg_items", map[string]interface{}{
			"fields": map[string]interface{}{
				"val": map[string]interface{}{"integerValue": strconv.Itoa(i + 1)},
			},
		})
	}

	stream, err := client.RunAggregationQuery(ctx, &firestorev1.RunAggregationQueryRequest{
		Parent: parent,
		QueryType: &firestorev1.RunAggregationQueryRequest_StructuredAggregationQuery{
			StructuredAggregationQuery: &firestorev1.StructuredAggregationQuery{
				QueryType: &firestorev1.StructuredAggregationQuery_StructuredQuery{
					StructuredQuery: &firestorev1.StructuredQuery{
						From: []*firestorev1.StructuredQuery_CollectionSelector{
							{CollectionId: "agg_items"},
						},
					},
				},
				Aggregations: []*firestorev1.StructuredAggregationQuery_Aggregation{
					{
						Operator: &firestorev1.StructuredAggregationQuery_Aggregation_Count_{
							Count: &firestorev1.StructuredAggregationQuery_Aggregation_Count{},
						},
						Alias: "count",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	var result *firestorev1.AggregationResult
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if r.GetResult() != nil {
			result = r.GetResult()
		}
	}
	require.NotNil(t, result, "expected an aggregation result")
	fields := result.GetAggregateFields()
	require.NotNil(t, fields["count"], "expected 'count' field in aggregate result")
	assert.Equal(t, int64(3), fields["count"].GetIntegerValue())
}

// TestRunAggregationQuery_Sum creates documents with integer values and verifies SUM.
func TestRunAggregationQuery_Sum(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)
	restBase := srv.restBase

	const db = "projects/p/databases/(default)"
	const parent = db + "/documents"

	for _, v := range []int{10, 20, 30} {
		createDoc(t, restBase, parent, "agg_sum_items", map[string]interface{}{
			"fields": map[string]interface{}{
				"amount": map[string]interface{}{"integerValue": strconv.Itoa(v)},
			},
		})
	}

	stream, err := client.RunAggregationQuery(ctx, &firestorev1.RunAggregationQueryRequest{
		Parent: parent,
		QueryType: &firestorev1.RunAggregationQueryRequest_StructuredAggregationQuery{
			StructuredAggregationQuery: &firestorev1.StructuredAggregationQuery{
				QueryType: &firestorev1.StructuredAggregationQuery_StructuredQuery{
					StructuredQuery: &firestorev1.StructuredQuery{
						From: []*firestorev1.StructuredQuery_CollectionSelector{
							{CollectionId: "agg_sum_items"},
						},
					},
				},
				Aggregations: []*firestorev1.StructuredAggregationQuery_Aggregation{
					{
						Operator: &firestorev1.StructuredAggregationQuery_Aggregation_Sum_{
							Sum: &firestorev1.StructuredAggregationQuery_Aggregation_Sum{
								Field: &firestorev1.StructuredQuery_FieldReference{FieldPath: "amount"},
							},
						},
						Alias: "total",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	var result *firestorev1.AggregationResult
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if r.GetResult() != nil {
			result = r.GetResult()
		}
	}
	require.NotNil(t, result, "expected an aggregation result")
	fields := result.GetAggregateFields()
	require.NotNil(t, fields["total"], "expected 'total' field in aggregate result")
	assert.Equal(t, int64(60), fields["total"].GetIntegerValue())
}

// TestListCollectionIds_Basic creates subcollections under a document and verifies
// that ListCollectionIds returns their IDs.
func TestListCollectionIds_Basic(t *testing.T) {
	srv := startTestServer(t)
	ctx := context.Background()
	client := grpcClient(t, srv)
	restBase := srv.restBase

	const db = "projects/p/databases/(default)"

	parentDocPath := createDoc(t, restBase, db+"/documents", "lcol_root", map[string]interface{}{})

	createDoc(t, restBase, parentDocPath, "sub_alpha", map[string]interface{}{})
	createDoc(t, restBase, parentDocPath, "sub_alpha", map[string]interface{}{})
	createDoc(t, restBase, parentDocPath, "sub_beta", map[string]interface{}{})

	resp, err := client.ListCollectionIds(ctx, &firestorev1.ListCollectionIdsRequest{
		Parent: parentDocPath,
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sub_alpha", "sub_beta"}, resp.GetCollectionIds())
}

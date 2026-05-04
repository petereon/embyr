package multitenant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
)

func firestoreClient(t *testing.T) firestorev1.FirestoreClient {
	t.Helper()
	if testGRPCAddr == "" {
		t.Skip("integration setup did not complete (Docker unavailable?)")
	}
	conn, err := grpc.NewClient(testGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return firestorev1.NewFirestoreClient(conn)
}

func strVal(s string) *firestorev1.Value {
	return &firestorev1.Value{ValueType: &firestorev1.Value_StringValue{StringValue: s}}
}

func TestAWSTenant_CRUD(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	parent := "projects/aws-proj/databases/aws-db/documents"

	created, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: "items",
		DocumentId:   "aws-doc-1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"name": strVal("Alice"),
				"src":  strVal("aws"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	t.Cleanup(func() {
		fs.DeleteDocument(context.Background(), &firestorev1.DeleteDocumentRequest{Name: created.Name}) //nolint:errcheck
	})

	got, err := fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.NoError(t, err)
	assert.Equal(t, "Alice", got.Fields["name"].GetStringValue())
	assert.Equal(t, "aws", got.Fields["src"].GetStringValue())

	_, err = fs.DeleteDocument(ctx, &firestorev1.DeleteDocumentRequest{Name: created.Name})
	require.NoError(t, err)

	_, err = fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGCPTenant_CRUD(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	parent := "projects/gcp-proj/databases/gcp-db/documents"

	created, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: "items",
		DocumentId:   "gcp-doc-1",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{
				"name": strVal("Bob"),
				"src":  strVal("gcp"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	t.Cleanup(func() {
		fs.DeleteDocument(context.Background(), &firestorev1.DeleteDocumentRequest{Name: created.Name}) //nolint:errcheck
	})

	got, err := fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.NoError(t, err)
	assert.Equal(t, "Bob", got.Fields["name"].GetStringValue())
	assert.Equal(t, "gcp", got.Fields["src"].GetStringValue())

	_, err = fs.DeleteDocument(ctx, &firestorev1.DeleteDocumentRequest{Name: created.Name})
	require.NoError(t, err)

	_, err = fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{Name: created.Name})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTenantIsolation(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	awsParent := "projects/aws-proj/databases/aws-db/documents"
	gcpParent := "projects/gcp-proj/databases/gcp-db/documents"

	awsDoc, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       awsParent,
		CollectionId: "shared-col",
		DocumentId:   "isolation-aws",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{"tenant": strVal("aws")},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := fs.DeleteDocument(context.Background(), &firestorev1.DeleteDocumentRequest{Name: awsDoc.Name}); err != nil {
			t.Logf("cleanup: delete %s: %v", awsDoc.Name, err)
		}
	})

	gcpDoc, err := fs.CreateDocument(ctx, &firestorev1.CreateDocumentRequest{
		Parent:       gcpParent,
		CollectionId: "shared-col",
		DocumentId:   "isolation-gcp",
		Document: &firestorev1.Document{
			Fields: map[string]*firestorev1.Value{"tenant": strVal("gcp")},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := fs.DeleteDocument(context.Background(), &firestorev1.DeleteDocumentRequest{Name: gcpDoc.Name}); err != nil {
			t.Logf("cleanup: delete %s: %v", gcpDoc.Name, err)
		}
	})

	awsList, err := fs.ListDocuments(ctx, &firestorev1.ListDocumentsRequest{
		Parent:       awsParent,
		CollectionId: "shared-col",
	})
	require.NoError(t, err)
	awsNames := docNames(awsList.Documents)
	assert.Len(t, awsNames, 1, "aws shared-col should contain exactly one document")
	assert.Contains(t, awsNames, awsDoc.Name)
	assert.NotContains(t, awsNames, gcpDoc.Name)

	gcpList, err := fs.ListDocuments(ctx, &firestorev1.ListDocumentsRequest{
		Parent:       gcpParent,
		CollectionId: "shared-col",
	})
	require.NoError(t, err)
	gcpNames := docNames(gcpList.Documents)
	assert.Len(t, gcpNames, 1, "gcp shared-col should contain exactly one document")
	assert.Contains(t, gcpNames, gcpDoc.Name)
	assert.NotContains(t, gcpNames, awsDoc.Name)
}

func TestUnknownTenant_Unauthenticated(t *testing.T) {
	ctx := context.Background()
	fs := firestoreClient(t)

	_, err := fs.GetDocument(ctx, &firestorev1.GetDocumentRequest{
		Name: "projects/unknown/databases/nope/documents/x/y",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func docNames(docs []*firestorev1.Document) []string {
	names := make([]string, len(docs))
	for i, d := range docs {
		names[i] = d.Name
	}
	return names
}

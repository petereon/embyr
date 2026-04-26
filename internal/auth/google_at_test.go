package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petereon/firstyr/internal/auth"
	"github.com/petereon/firstyr/internal/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// #GOOGLE-AT — A valid OAuth access token (response shape: scope/audience/azp,
// no aud == projectID) must NOT be rejected outright. The current
// implementation only sends ?id_token=… so access tokens always 400 from the
// endpoint and the user gets Unauthenticated. The fix tries id_token first,
// falls back to access_token.
func TestGoogleAuth_AccessTokenAccepted(t *testing.T) {
	const projectID = "my-project"
	const accessToken = "ya29.access-token-payload"
	const idToken = "eyJ.fake-id-token.sig"

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Real Google: ?id_token=<jwt> for ID tokens, ?access_token=<opaque>
		// for OAuth access tokens. Returns 400 if the wrong parameter is used.
		q := r.URL.Query()
		switch {
		case q.Get("id_token") == idToken:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"aud":      projectID,
				"audience": projectID,
			})
		case q.Get("access_token") == accessToken:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				// Real Google access tokens are issued for a Google Cloud
				// project; the response does not echo the project_id directly.
				// The validator must accept any 200 response when the project
				// ID match isn't possible from the access_token shape.
				"scope":     "https://www.googleapis.com/auth/datastore",
				"audience":  "1234567890-clientid.apps.googleusercontent.com",
				"expires_in": 3000,
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_token"}`))
		}
	}))
	t.Cleanup(mock.Close)

	auth.SetTokenInfoURL(mock.URL)

	cfg := &config.Config{}
	cfg.Auth.Mode = "google"
	cfg.Auth.GoogleProjectID = projectID
	a, err := auth.New(cfg, zap.NewNop())
	require.NoError(t, err)

	makeCtx := func(token string) context.Context {
		md := metadata.New(map[string]string{"authorization": "Bearer " + token})
		return metadata.NewIncomingContext(context.Background(), md)
	}

	called := false
	handler := func(ctx context.Context, _ any) (any, error) {
		called = true
		return "ok", nil
	}

	// Case 1: ID token with matching aud — must succeed.
	called = false
	_, err = a.Unary(makeCtx(idToken), nil, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err, "id token with matching aud must be accepted")
	require.True(t, called)

	// Case 2: OAuth access token — current code sends id_token=, which 400s,
	// so the validator rejects it as Unauthenticated. The fix must fall back
	// to access_token=.
	called = false
	_, err = a.Unary(makeCtx(accessToken), nil, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err, "OAuth access token must be accepted via access_token fallback")
	require.True(t, called)

	// Case 3: garbage token — must still 401.
	called = false
	_, err = a.Unary(makeCtx("garbage"), nil, &grpc.UnaryServerInfo{}, handler)
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.False(t, called)
}

package webchannel_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/webchannel"
	"github.com/stretchr/testify/require"
)

// #DOSPANIC — A Listen GET on a SID belonging to a Write session must not panic.
//
// We start a Write-channel session (POST to /Write/channel), then issue a
// Listen-channel GET with that same SID. The Listen handler must respond with
// 400 (or any non-panic status) instead of crashing the server.
func TestWebChannel_CrossHandler_GET_DoesNotPanic(t *testing.T) {
	mgr := webchannel.NewManager()

	// Listen handler: a no-op listenFn (we never expect it to be reached for a
	// Write session SID).
	listenFn := func(stream firestorev1.Firestore_ListenServer) error {
		<-stream.Context().Done()
		return nil
	}
	writeFn := func(stream firestorev1.Firestore_WriteServer) error {
		<-stream.Context().Done()
		return nil
	}
	listenH := webchannel.NewHandler(mgr, listenFn)
	writeH := webchannel.NewWriteHandler(mgr, writeFn)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/Write/channel"):
			writeH.ServeHTTP(w, r)
		default:
			listenH.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	// Establish a Write session via POST.
	form := url.Values{
		"count":            {"0"},
		"ofs":              {"0"},
		"req0___data__":   {`{"writes":[]}`},
	}
	postResp, err := http.PostForm(srv.URL+"/Write/channel?VER=8&RID=1", form)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, postResp.StatusCode)
	defer postResp.Body.Close()

	// Extract the SID from the Connect chunk in the response body.
	bodyBuf := make([]byte, 1024)
	n, _ := postResp.Body.Read(bodyBuf)
	body := string(bodyBuf[:n])
	require.Contains(t, body, `"c"`, "establishment response must include connect chunk")

	// We can't easily parse SID out of the chunk format here, so just enumerate
	// active session IDs from the manager.
	sid := findFirstSessionID(mgr)
	require.NotEmpty(t, sid, "Write POST must register a session")

	// Now issue a Listen GET with that SID. The handler must reject the
	// cross-channel request with 400 BadRequest, not panic.
	getCtx, getCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer getCancel()
	req, _ := http.NewRequestWithContext(getCtx, http.MethodGet,
		srv.URL+"/Listen/channel?SID="+sid+"&AID=0&CI=0", nil)
	getResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusBadRequest, getResp.StatusCode,
		"Listen GET on a Write SID must be rejected as BadRequest, not crash the handler")
}

// #DOSPANIC (companion) — A Write GET on a Listen session SID must also not panic.
func TestWebChannel_CrossHandler_WriteGet_OnListenSession_DoesNotPanic(t *testing.T) {
	mgr := webchannel.NewManager()
	listenFn := func(s firestorev1.Firestore_ListenServer) error { <-s.Context().Done(); return nil }
	writeFn := func(s firestorev1.Firestore_WriteServer) error { <-s.Context().Done(); return nil }
	listenH := webchannel.NewHandler(mgr, listenFn)
	writeH := webchannel.NewWriteHandler(mgr, writeFn)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/Write/channel"):
			writeH.ServeHTTP(w, r)
		default:
			listenH.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	form := url.Values{
		"count":          {"0"},
		"ofs":            {"0"},
		"req0___data__": {`{"database":"projects/p/databases/(default)"}`},
	}
	postResp, err := http.PostForm(srv.URL+"/Listen/channel?VER=8&RID=1", form)
	require.NoError(t, err)
	postResp.Body.Close()

	sid := findFirstSessionID(mgr)
	require.NotEmpty(t, sid)

	getCtx, getCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer getCancel()
	req, _ := http.NewRequestWithContext(getCtx, http.MethodGet,
		srv.URL+"/Write/channel?SID="+sid+"&AID=0&CI=0", nil)
	getResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusBadRequest, getResp.StatusCode,
		"Write GET on a Listen SID must be rejected as BadRequest, not crash the handler")
}

// #SHUT1 — Manager.Shutdown must cancel every active session's bridge context
// so that gRPC handler goroutines and pumps exit when the server shuts down.
func TestWebChannel_Manager_Shutdown_CancelsActiveSessions(t *testing.T) {
	mgr := webchannel.NewManager()

	listenStarted := make(chan struct{}, 1)
	var listenReturned atomic.Bool
	listenFn := func(stream firestorev1.Firestore_ListenServer) error {
		listenStarted <- struct{}{}
		<-stream.Context().Done()
		listenReturned.Store(true)
		return nil
	}
	listenH := webchannel.NewHandler(mgr, listenFn)
	srv := httptest.NewServer(listenH)
	t.Cleanup(srv.Close)

	form := url.Values{
		"count":          {"0"},
		"ofs":            {"0"},
		"req0___data__": {`{"database":"projects/p/databases/(default)"}`},
	}
	resp, err := http.PostForm(srv.URL+"?VER=8&RID=1", form)
	require.NoError(t, err)
	resp.Body.Close()

	select {
	case <-listenStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("listenFn did not start")
	}

	// Now call Shutdown — the gRPC handler goroutine must return.
	mgr.Shutdown()

	require.Eventually(t, listenReturned.Load, 2*time.Second, 10*time.Millisecond,
		"listen handler must return after Manager.Shutdown")
}

// #CONNSEQ — connSeq seeded from DataSeq() must not collide with subsequent
// data chunks emitted by the pump. Concretely: when DataSeq()=2, and the noop
// counter starts at 2 and increments to 3, a subsequent FormatDataChunk must
// NOT also produce seq=3.
//
// The proper fix is to claim noop seqs from the same global counter as data
// chunks (via Add(1)), so that no two chunks in a session ever share a seq.
func TestSession_NoopAndDataNeverCollide(t *testing.T) {
	mgr := webchannel.NewManager()
	sess := mgr.NewSession()
	sess.FormatConnectChunk() // seq=2 ready for first data

	// First data chunk: seq=2.
	d1 := string(sess.FormatDataChunk([]byte(`{}`)))
	require.Contains(t, d1, `[2,`)

	// A back-channel-style noop using NextNoopSeq() must claim a fresh seq.
	noopSeq := sess.NextNoopSeq()

	// Subsequent data chunks must have seqs that are strictly greater than the
	// noop seq.
	d2 := string(sess.FormatDataChunk([]byte(`{}`)))
	dataSeq := extractFirstSeq(t, d2)
	require.Greater(t, dataSeq, noopSeq,
		"subsequent data seq (%d) must be > noop seq (%d) — no collision allowed", dataSeq, noopSeq)
}

// findFirstSessionID returns the SID of any active session in the manager,
// or "" if none exist. Useful for tests that establish a session via HTTP
// and then need its SID.
func findFirstSessionID(mgr *webchannel.Manager) string {
	return mgr.AnySessionID()
}

// extractFirstSeq parses the leading sequence number out of a BrowserChannel
// chunk of the form "<len>\n[[<seq>, [...]]]".
func extractFirstSeq(t *testing.T, chunk string) int64 {
	t.Helper()
	idx := strings.Index(chunk, "[[")
	require.GreaterOrEqual(t, idx, 0, "chunk must contain '[['")
	rest := chunk[idx+2:]
	commaIdx := strings.Index(rest, ",")
	require.GreaterOrEqual(t, commaIdx, 0)
	var n int64
	for _, c := range rest[:commaIdx] {
		require.True(t, c >= '0' && c <= '9')
		n = n*10 + int64(c-'0')
	}
	return n
}

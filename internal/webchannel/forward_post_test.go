package webchannel_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/webchannel"
	"github.com/stretchr/testify/require"
)

// Forward POST response must be in WebChannel chunk-framed format:
//
//	<byte_length>\n<json_array>
//
// where the JSON is a 3-element array. Without the length prefix the SDK's
// chunk extractor (Sb in @firebase/webchannel-wrapper) returns "incomplete"
// and marks the request failed, which blocks every subsequent forward send
// (manifests as filter switches that hang or sessions reconnecting every
// ~10s in demo-react).
func TestWebChannel_ForwardPostResponse_IsChunkFramed3ElementArray(t *testing.T) {
	mgr := webchannel.NewManager()
	listenFn := func(s firestorev1.Firestore_ListenServer) error {
		<-s.Context().Done()
		return nil
	}
	srv := httptest.NewServer(webchannel.NewHandler(mgr, listenFn))
	t.Cleanup(srv.Close)

	// 1) Establish a session via new-session POST.
	form := url.Values{
		"count":         {"0"},
		"ofs":           {"0"},
		"req0___data__": {`{"database":"projects/demo/databases/(default)"}`},
	}
	resp, err := http.PostForm(srv.URL+"?VER=8&RID=1", form)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()

	// Extract the SID from the connect chunk (`[[0,["c","<sid>",...]],...]`).
	sid := mgr.AnySessionID()
	require.NotEmpty(t, sid, "new-session POST must register a session")
	require.Contains(t, string(body), `"c"`, "new-session response carries the connect chunk")

	// 2) Send a follow-up forward POST and inspect the response framing.
	postCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(postCtx, http.MethodPost,
		srv.URL+"?VER=8&RID=2&SID="+sid+"&AID=1",
		strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	resp2.Body.Close()

	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.NotEmpty(t, body2, "forward POST response must have a body — empty body breaks the SDK's chunk extractor")

	// Parse the chunk framing: <length>\n<json>.
	nl := strings.IndexByte(string(body2), '\n')
	require.GreaterOrEqual(t, nl, 1,
		"forward POST response must be `<length>\\n<json>` chunk format; got %q", body2)

	declaredLen, err := strconv.Atoi(string(body2[:nl]))
	require.NoError(t, err, "length prefix must be numeric; got %q", body2[:nl])

	jsonPart := string(body2[nl+1:])
	require.Equal(t, declaredLen, len(jsonPart),
		"declared chunk length must equal the JSON byte length (chunk framing contract)")

	// JSON body must be a 3-element array per the SDK's Rb dispatcher.
	var arr []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(jsonPart), &arr),
		"chunk content must be valid JSON array; got %q", jsonPart)
	require.Len(t, arr, 3,
		"forward POST response JSON must be a 3-element array [lastArrayId, outstandingBytes, _]; got %v", arr)
}

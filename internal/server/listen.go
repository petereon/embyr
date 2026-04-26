package server

import (
	"io"
	"time"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/petereon/firstyr/internal/listen"
	"github.com/petereon/firstyr/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Listen implements the Firestore bidirectional streaming Listen RPC.
// It delivers the initial snapshot of each target, then live change events.
func (s *firestoreServer) Listen(stream firestorev1.Firestore_ListenServer) error {
	ctx := stream.Context()

	// Map of target_id → per-target descriptor.
	// Exactly one of query or docPaths is non-nil.
	type targetInfo struct {
		parent       string
		collectionID string
		targetID     int32
		query        *store.Query // set for QueryTarget
		docPaths     []string     // set for DocumentsTarget
	}
	targets := make(map[int32]targetInfo)

	// Subscribe to all changes from the storage adapter via the registry.
	subscription := s.registry.SubscribeWithSignal()
	defer subscription.Cancel()
	changeCh := subscription.Ch()

	// recvCh receives ListenRequests from the client asynchronously.
	recvCh := make(chan *firestorev1.ListenRequest, 8)
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				select {
				case recvErrCh <- err:
				case <-ctx.Done():
				}
				return
			}
			// Both sends must respect ctx.Done so we don't leak when the main
			// loop has already returned (e.g., client cancellation while the
			// recv buffer is full).
			select {
			case recvCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	sendTargetChange := func(changeType firestorev1.TargetChange_TargetChangeType, targetIDs []int32, resumeToken []byte) error {
		tc := &firestorev1.TargetChange{
			TargetChangeType: changeType,
			TargetIds:        targetIDs,
			ReadTime:         timestamppb.Now(),
		}
		if len(resumeToken) > 0 {
			tc.ResumeToken = resumeToken
		}
		return stream.Send(&firestorev1.ListenResponse{
			ResponseType: &firestorev1.ListenResponse_TargetChange{TargetChange: tc},
		})
	}

	deliverSnapshot := func(ti targetInfo) error {
		if err := sendTargetChange(firestorev1.TargetChange_ADD, []int32{ti.targetID}, nil); err != nil {
			return err
		}

		readTime := time.Now().UTC()

		if len(ti.docPaths) > 0 {
			// DocumentsTarget: fetch each listed path individually.
			for _, path := range ti.docPaths {
				sd, err := s.db.GetDocument(ctx, path)
				if err != nil {
					if status.Code(err) == codes.NotFound {
						continue // document doesn't exist — skip
					}
					return err
				}
				proto, err := codec.StoreToProto(sd)
				if err != nil {
					return status.Errorf(codes.Internal, "decode document: %v", err)
				}
				if err := stream.Send(&firestorev1.ListenResponse{
					ResponseType: &firestorev1.ListenResponse_DocumentChange{
						DocumentChange: &firestorev1.DocumentChange{
							Document:  proto,
							TargetIds: []int32{ti.targetID},
						},
					},
				}); err != nil {
					return err
				}
			}
		} else {
			// QueryTarget: stream current documents via paginated query.
			q := ti.query
			for {
				page, err := s.db.QueryDocuments(ctx, q)
				if err != nil {
					return err
				}
				for _, sd := range page.Documents {
					proto, err := codec.StoreToProto(sd)
					if err != nil {
						return status.Errorf(codes.Internal, "decode document: %v", err)
					}
					if err := stream.Send(&firestorev1.ListenResponse{
						ResponseType: &firestorev1.ListenResponse_DocumentChange{
							DocumentChange: &firestorev1.DocumentChange{
								Document:  proto,
								TargetIds: []int32{ti.targetID},
							},
						},
					}); err != nil {
						return err
					}
				}
				if page.NextPageToken == "" || q.Limit > 0 {
					break
				}
				q = &store.Query{
					Parent:       q.Parent,
					CollectionID: q.CollectionID,
					Filter:       q.Filter,
					OrderBy:      q.OrderBy,
					Limit:        q.Limit,
					PageSize:     q.PageSize,
					PageToken:    page.NextPageToken,
				}
			}
		}

		if err := sendTargetChange(
			firestorev1.TargetChange_CURRENT,
			[]int32{ti.targetID},
			listen.EncodeResumeToken(readTime),
		); err != nil {
			return err
		}
		// Global NO_CHANGE seals the snapshot: SDK waits for this before
		// resolving getDoc / onSnapshot for the first time.
		return sendTargetChange(firestorev1.TargetChange_NO_CHANGE, nil,
			listen.EncodeResumeToken(time.Now().UTC()))
	}

	// Keep-alive ticker: send a NO_CHANGE every 30s to prevent proxy timeouts.
	keepAlive := time.NewTicker(30 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-recvErrCh:
			if err == io.EOF {
				return nil
			}
			return err

		case req := <-recvCh:
			switch tc := req.GetTargetChange().(type) {
			case *firestorev1.ListenRequest_AddTarget:
				t := tc.AddTarget
				if _, exists := targets[t.GetTargetId()]; exists {
					// Re-add of an existing target: remove it first (real emulator behaviour).
					delete(targets, t.GetTargetId())
					if err := sendTargetChange(firestorev1.TargetChange_REMOVE, []int32{t.GetTargetId()}, nil); err != nil {
						return err
					}
				}
				var ti targetInfo
				ti.targetID = t.GetTargetId()
				switch tp := t.GetTargetType().(type) {
				case *firestorev1.Target_Documents:
					ti.docPaths = tp.Documents.GetDocuments()
				case *firestorev1.Target_Query:
					qt := tp.Query
					sq := qt.GetStructuredQuery()
					if sq == nil || len(sq.GetFrom()) == 0 {
						return status.Error(codes.InvalidArgument, "structured_query.from is required")
					}
					q, err := codec.QueryFromStructuredQuery(qt.GetParent(), sq, 300, "")
					if err != nil {
						return err
					}
					ti.parent = qt.GetParent()
					ti.collectionID = sq.GetFrom()[0].GetCollectionId()
					ti.query = q
				default:
					return status.Error(codes.InvalidArgument, "target must be query or documents type")
				}
				targets[ti.targetID] = ti
				if err := deliverSnapshot(ti); err != nil {
					return err
				}

			case *firestorev1.ListenRequest_RemoveTarget:
				delete(targets, tc.RemoveTarget)
				if err := sendTargetChange(firestorev1.TargetChange_REMOVE, []int32{tc.RemoveTarget}, nil); err != nil {
					return err
				}
			}

		case change := <-changeCh:
			// If the registry dropped any changes for this subscription, the
			// targets' snapshots may be stale. Send RESET so the client
			// re-bootstraps every active target before delivering more events.
			if subscription.Overflowed() {
				ids := make([]int32, 0, len(targets))
				for tid := range targets {
					ids = append(ids, tid)
				}
				if err := sendTargetChange(firestorev1.TargetChange_RESET, ids, nil); err != nil {
					return err
				}
				// Re-snapshot every target so the client gets a consistent view.
				for _, ti := range targets {
					if err := deliverSnapshot(ti); err != nil {
						return err
					}
				}
				continue
			}
			// Fan out to every matching target.
			for _, ti := range targets {
				var matches bool
				if len(ti.docPaths) > 0 {
					// DocumentsTarget: match by exact path.
					for _, p := range ti.docPaths {
						if p == change.Path {
							matches = true
							break
						}
					}
				} else {
					// QueryTarget: match by parent + collection.
					matches = ti.parent == change.Parent && ti.collectionID == change.Collection
				}
				if !matches {
					continue
				}
				if change.Kind == store.DocChangeDelete {
					if err := stream.Send(&firestorev1.ListenResponse{
						ResponseType: &firestorev1.ListenResponse_DocumentDelete{
							DocumentDelete: &firestorev1.DocumentDelete{
								Document:         change.Path,
								RemovedTargetIds: []int32{ti.targetID},
								ReadTime:         timestamppb.Now(),
							},
						},
					}); err != nil {
						return err
					}
				} else {
					sd, err := s.db.GetDocument(ctx, change.Path)
					if err != nil {
						if status.Code(err) == codes.NotFound {
							continue
						}
						return err
					}
					proto, err := codec.StoreToProto(sd)
					if err != nil {
						return status.Errorf(codes.Internal, "decode document: %v", err)
					}
					// For query targets, apply filter; document targets always forward.
					if ti.query != nil && !codec.MatchesFilter(proto, ti.query.Filter) {
						continue
					}
					if err := stream.Send(&firestorev1.ListenResponse{
						ResponseType: &firestorev1.ListenResponse_DocumentChange{
							DocumentChange: &firestorev1.DocumentChange{
								Document:  proto,
								TargetIds: []int32{ti.targetID},
							},
						},
					}); err != nil {
						return err
					}
				}
				// Send a NO_CHANGE resume token after each delivered change.
				if err := sendTargetChange(firestorev1.TargetChange_NO_CHANGE, nil,
					listen.EncodeResumeToken(time.Now().UTC())); err != nil {
					return err
				}
			}

		case <-keepAlive.C:
			if err := sendTargetChange(firestorev1.TargetChange_NO_CHANGE, nil, nil); err != nil {
				return err
			}
		}
	}
}

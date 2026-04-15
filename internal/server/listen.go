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

	// Map of target_id → collectionInfo for filtering incoming changes.
	type targetInfo struct {
		parent       string
		collectionID string
		targetID     int32
	}
	targets := make(map[int32]targetInfo)

	// Subscribe to all changes from the storage adapter via the registry.
	changeCh, unsub := s.registry.Subscribe()
	defer unsub()

	// recvCh receives ListenRequests from the client asynchronously.
	recvCh := make(chan *firestorev1.ListenRequest, 8)
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			recvCh <- req
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
		// Announce the target was added.
		if err := sendTargetChange(firestorev1.TargetChange_ADD, []int32{ti.targetID}, nil); err != nil {
			return err
		}

		// Stream current documents.
		q := &store.Query{
			Parent:       ti.parent,
			CollectionID: ti.collectionID,
			PageSize:     300,
		}
		readTime := time.Now().UTC()
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
			if page.NextPageToken == "" {
				break
			}
			q.PageToken = page.NextPageToken
		}

		// Signal snapshot complete.
		return sendTargetChange(
			firestorev1.TargetChange_CURRENT,
			[]int32{ti.targetID},
			listen.EncodeResumeToken(readTime),
		)
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
				qt := t.GetQuery()
				if qt == nil {
					return status.Error(codes.Unimplemented, "only query targets are supported")
				}
				sq := qt.GetStructuredQuery()
				if sq == nil || len(sq.GetFrom()) == 0 {
					return status.Error(codes.InvalidArgument, "structured_query.from is required")
				}
				ti := targetInfo{
					parent:       qt.GetParent(),
					collectionID: sq.GetFrom()[0].GetCollectionId(),
					targetID:     t.GetTargetId(),
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
			// Fan out to every target that matches this change's collection + parent.
			for _, ti := range targets {
				if ti.parent != change.Parent || ti.collectionID != change.Collection {
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
					// Fetch the full document (change.Data may be truncated for large docs).
					sd, err := s.db.GetDocument(ctx, change.Path)
					if err != nil {
						if status.Code(err) == codes.NotFound {
							continue // deleted between notify and fetch
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
				// Send a NO_CHANGE resume token after each change batch.
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

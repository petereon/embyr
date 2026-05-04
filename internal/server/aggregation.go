package server

import (
	"context"

	firestorev1 "github.com/petereon/embyr/gen/go/google/firestore/v1"
	"github.com/petereon/embyr/internal/codec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunAggregationQuery executes COUNT/SUM/AVG aggregations over a structured query.
func (s *firestoreServer) RunAggregationQuery(req *firestorev1.RunAggregationQueryRequest, stream firestorev1.Firestore_RunAggregationQueryServer) error {
	ctx := stream.Context()

	saq := req.GetStructuredAggregationQuery()
	if saq == nil {
		return status.Error(codes.InvalidArgument, "structured_aggregation_query is required")
	}

	q, err := codec.AggregationQueryFromProto(req.GetParent(), saq)
	if err != nil {
		return err
	}

	results, err := s.adapter(ctx).RunAggregationQuery(ctx, q)
	if err != nil {
		return err
	}

	// Convert store.AggregateValue map to proto Value map.
	aggFields := make(map[string]*firestorev1.Value, len(results))
	for alias, av := range results {
		switch {
		case av.IsNull:
			aggFields[alias] = &firestorev1.Value{ValueType: &firestorev1.Value_NullValue{}}
		case av.IsInt:
			aggFields[alias] = &firestorev1.Value{ValueType: &firestorev1.Value_IntegerValue{IntegerValue: av.IntVal}}
		default:
			aggFields[alias] = &firestorev1.Value{ValueType: &firestorev1.Value_DoubleValue{DoubleValue: av.FloatVal}}
		}
	}

	return stream.Send(&firestorev1.RunAggregationQueryResponse{
		Result:   &firestorev1.AggregationResult{AggregateFields: aggFields},
		ReadTime: timestamppb.Now(),
	})
}

// ListCollectionIds returns the distinct collection IDs of immediate child
// collections of the document at the requested parent path.
func (s *firestoreServer) ListCollectionIds(ctx context.Context, req *firestorev1.ListCollectionIdsRequest) (*firestorev1.ListCollectionIdsResponse, error) {
	parent := req.GetParent()
	if parent == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}

	ids, nextToken, err := s.adapter(ctx).ListCollectionIds(ctx, parent, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}

	return &firestorev1.ListCollectionIdsResponse{
		CollectionIds: ids,
		NextPageToken: nextToken,
	}, nil
}

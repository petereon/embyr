package codec_test

import (
	"fmt"
	"testing"

	firestorev1 "github.com/petereon/firstyr/gen/go/google/firestore/v1"
	"github.com/petereon/firstyr/internal/codec"
	"github.com/stretchr/testify/require"
)

// #ALIAS — When the client sends 10+ aggregations with no explicit alias, the
// default alias must be `field_<i>` (decimal), not `field_<rune('0'+i)>`
// (which produces ':', ';', '<', … past i=9).
func TestAggregationQuery_DefaultAlias_PastNine(t *testing.T) {
	saq := &firestorev1.StructuredAggregationQuery{
		QueryType: &firestorev1.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorev1.StructuredQuery{
				From: []*firestorev1.StructuredQuery_CollectionSelector{{CollectionId: "c"}},
			},
		},
	}
	for i := 0; i < 12; i++ {
		saq.Aggregations = append(saq.Aggregations,
			&firestorev1.StructuredAggregationQuery_Aggregation{
				Operator: &firestorev1.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorev1.StructuredAggregationQuery_Aggregation_Count{},
				},
			})
	}

	q, err := codec.AggregationQueryFromProto("projects/p/databases/d/documents", saq)
	require.NoError(t, err)
	require.Len(t, q.Aggregations, 12)

	// Every alias must be of the form field_<digits> with the correct integer.
	for i, a := range q.Aggregations {
		require.Equal(t, fmt.Sprintf("field_%d", i), a.Alias,
			"default alias for index %d must be %q, got %q", i, fmt.Sprintf("field_%d", i), a.Alias)
	}
}

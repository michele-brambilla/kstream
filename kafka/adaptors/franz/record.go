package franz

import (
	"context"
	"fmt"
	"time"

	"github.com/michele-brambilla/kstream/v2/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// record wraps a kgo.Record and implements kafka.Record.
type record struct {
	ctx  context.Context
	kRec *kgo.Record
}

func (r *record) Ctx() context.Context { return r.ctx }

func (r *record) WithCtx(ctx context.Context) kafka.Record {
	return &record{ctx: ctx, kRec: r.kRec}
}

func (r *record) Key() []byte          { return r.kRec.Key }
func (r *record) Value() []byte        { return r.kRec.Value }
func (r *record) Topic() string        { return r.kRec.Topic }
func (r *record) Partition() int32     { return r.kRec.Partition }
func (r *record) Offset() int64        { return r.kRec.Offset }
func (r *record) Timestamp() time.Time { return r.kRec.Timestamp }

func (r *record) Headers() kafka.RecordHeaders {
	headers := make(kafka.RecordHeaders, len(r.kRec.Headers))
	for i, h := range r.kRec.Headers {
		headers[i] = kafka.RecordHeader{
			Key:   []byte(h.Key),
			Value: h.Value,
		}
	}
	return headers
}

func (r *record) String() string {
	return fmt.Sprintf(`%s[%d]@%d`, r.Topic(), r.Partition(), r.Offset())
}

// newRecord creates a kafka.Record from key/value/topic/partition/timestamp/headers/meta.
// This is used by Producer.NewRecord.
type producerRecord struct {
	ctx       context.Context
	key       []byte
	value     []byte
	topic     string
	partition int32
	timestamp time.Time
	headers   kafka.RecordHeaders
	meta      string
	// kRec is populated lazily for produce calls.
	kRec *kgo.Record
}

func (r *producerRecord) Ctx() context.Context { return r.ctx }

func (r *producerRecord) WithCtx(ctx context.Context) kafka.Record {
	cp := *r
	cp.ctx = ctx
	return &cp
}

func (r *producerRecord) Key() []byte                  { return r.key }
func (r *producerRecord) Value() []byte                { return r.value }
func (r *producerRecord) Topic() string                { return r.topic }
func (r *producerRecord) Partition() int32             { return r.partition }
func (r *producerRecord) Offset() int64                { return 0 }
func (r *producerRecord) Timestamp() time.Time         { return r.timestamp }
func (r *producerRecord) Headers() kafka.RecordHeaders { return r.headers }
func (r *producerRecord) String() string {
	return fmt.Sprintf(`%s[%d]@producer`, r.topic, r.partition)
}

// toKgo converts a kafka.Record to a kgo.Record suitable for producing.
func toKgo(rec kafka.Record) *kgo.Record {
	if pr, ok := rec.(*producerRecord); ok && pr.kRec != nil {
		return pr.kRec
	}

	kr := &kgo.Record{
		Key:       rec.Key(),
		Value:     rec.Value(),
		Topic:     rec.Topic(),
		Timestamp: rec.Timestamp(),
	}

	partition := rec.Partition()
	if partition >= 0 {
		kr.Partition = partition
	}

	for _, h := range rec.Headers() {
		kr.Headers = append(kr.Headers, kgo.RecordHeader{
			Key:   string(h.Key),
			Value: h.Value,
		})
	}

	return kr
}

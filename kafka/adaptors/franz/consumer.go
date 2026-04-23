package franz

import (
	"context"
	"fmt"
	"sync"

	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// GroupConsumerConfig
// ---------------------------------------------------------------------------

// GroupConsumerConfig wraps kafka.GroupConsumerConfig for the franz adaptor.
type GroupConsumerConfig struct {
	*kafka.GroupConsumerConfig
	// ExtraOpts allows passing additional kgo.Opt options.
	ExtraOpts []kgo.Opt
}

func NewGroupConsumerConfig() *GroupConsumerConfig {
	return &GroupConsumerConfig{
		GroupConsumerConfig: kafka.NewConfig(),
	}
}

func (c *GroupConsumerConfig) copy() *GroupConsumerConfig {
	return &GroupConsumerConfig{
		GroupConsumerConfig: c.GroupConsumerConfig.Copy(),
		ExtraOpts:           append([]kgo.Opt(nil), c.ExtraOpts...),
	}
}

// ---------------------------------------------------------------------------
// groupConsumerProvider — implements kafka.GroupConsumerProvider
// ---------------------------------------------------------------------------

type groupConsumerProvider struct {
	config *GroupConsumerConfig
}

func NewGroupConsumerProvider(config *GroupConsumerConfig) kafka.GroupConsumerProvider {
	return &groupConsumerProvider{config: config}
}

func (p *groupConsumerProvider) NewBuilder(conf *kafka.GroupConsumerConfig) kafka.GroupConsumerBuilder {
	p.config.GroupConsumerConfig = conf

	return func(configure func(*kafka.GroupConsumerConfig)) (kafka.GroupConsumer, error) {
		cfgCopy := p.config.copy()
		configure(cfgCopy.GroupConsumerConfig)
		return newGroupConsumer(cfgCopy)
	}
}

// ---------------------------------------------------------------------------
// groupConsumer — implements kafka.GroupConsumer
// ---------------------------------------------------------------------------

type groupConsumer struct {
	client   *kgo.Client
	config   *GroupConsumerConfig
	handler  kafka.RebalanceHandler
	errs     chan error
	stopOnce sync.Once
	stopCh   chan struct{}
}

func newGroupConsumer(config *GroupConsumerConfig) (kafka.GroupConsumer, error) {
	offset := kgo.NewOffset().AtEnd()
	if config.Offsets.Initial == kafka.OffsetEarliest {
		offset = kgo.NewOffset().AtStart()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.BootstrapServers...),
		kgo.ClientID(config.Id),
		kgo.ConsumerGroup(config.GroupId),
		kgo.ConsumeResetOffset(offset),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			_ = cl.CommitUncommittedOffsets(ctx)
		}),
	}

	if config.EOSEnabled {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	opts = append(opts, config.ExtraOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, `franz group consumer init failed`)
	}

	return &groupConsumer{
		client: client,
		config: config,
		errs:   make(chan error, 8),
		stopCh: make(chan struct{}),
	}, nil
}

func (g *groupConsumer) Subscribe(topics []string, handler kafka.RebalanceHandler) error {
	g.handler = handler
	g.client.AddConsumeTopics(topics...)
	go g.consumeLoop()
	return nil
}

func (g *groupConsumer) consumeLoop() {
	ctx := context.Background()
	for {
		select {
		case <-g.stopCh:
			return
		default:
		}

		fetches := g.client.PollRecords(ctx, g.config.ConsumerMessageChanSize)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(t string, p int32, err error) {
			g.errs <- fmt.Errorf("kafka fetch error [%s#%d]: %w", t, p, err)
		})

		// Group records by TopicPartition for handler dispatch.
		byPartition := make(map[kafka.TopicPartition][]*kgo.Record)
		fetches.EachRecord(func(r *kgo.Record) {
			tp := kafka.TopicPartition{Topic: r.Topic, Partition: r.Partition}
			byPartition[tp] = append(byPartition[tp], r)
		})

		if len(byPartition) == 0 {
			continue
		}

		// Build session / assignment covering this fetch batch.
		tps := make(kafka.TopicPartitions, 0, len(byPartition))
		for tp := range byPartition {
			tps = append(tps, tp)
		}

		sess := &groupSession{
			client:  g.client,
			tps:     tps,
			assign:  &assignment{tps: tps},
			groupID: g.config.GroupId,
		}

		if g.handler != nil {
			if err := g.handler.OnPartitionAssigned(ctx, sess); err != nil {
				g.errs <- errors.Wrap(err, `OnPartitionAssigned error`)
			}
		}

		var wg sync.WaitGroup
		for tp, recs := range byPartition {
			tp := tp
			recs := recs
			claim := newPartitionClaim(tp, len(recs))

			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, r := range recs {
					rec := &record{ctx: ctx, kRec: r}
					if g.config.ContextExtractor != nil {
						rec.ctx = g.config.ContextExtractor(rec)
					}
					var kr kafka.Record = rec
					if g.config.Interceptor != nil {
						kr = g.config.Interceptor.OnConsume(rec)
					}
					claim.messages <- kr
				}
				close(claim.messages)
				if g.handler != nil {
					if err := g.handler.Consume(ctx, sess, claim); err != nil {
						g.errs <- errors.Wrap(err, `Consume error`)
					}
				}
			}()
		}
		wg.Wait()

		if g.handler != nil {
			if err := g.handler.OnPartitionRevoked(ctx, sess); err != nil {
				g.errs <- errors.Wrap(err, `OnPartitionRevoked error`)
			}
		}
	}
}

func (g *groupConsumer) Unsubscribe() error {
	g.stopOnce.Do(func() { close(g.stopCh) })
	g.client.Close()
	return nil
}

func (g *groupConsumer) Errors() <-chan error { return g.errs }

// ---------------------------------------------------------------------------
// partitionClaim — implements kafka.PartitionClaim
// ---------------------------------------------------------------------------

type partitionClaim struct {
	tp       kafka.TopicPartition
	messages chan kafka.Record
}

func newPartitionClaim(tp kafka.TopicPartition, bufSize int) *partitionClaim {
	if bufSize < 1 {
		bufSize = 1
	}
	return &partitionClaim{tp: tp, messages: make(chan kafka.Record, bufSize)}
}

func (c *partitionClaim) TopicPartition() kafka.TopicPartition { return c.tp }
func (c *partitionClaim) Records() <-chan kafka.Record          { return c.messages }

// ---------------------------------------------------------------------------
// assignment — implements kafka.Assignment
// ---------------------------------------------------------------------------

type assignment struct {
	tps    kafka.TopicPartitions
	resets map[kafka.TopicPartition]kafka.Offset
}

func (a *assignment) TPs() kafka.TopicPartitions { return a.tps }

func (a *assignment) ResetOffset(tp kafka.TopicPartition, offset kafka.Offset) {
	if a.resets == nil {
		a.resets = make(map[kafka.TopicPartition]kafka.Offset)
	}
	a.resets[tp] = offset
}

// ---------------------------------------------------------------------------
// groupSession — implements kafka.GroupSession
// ---------------------------------------------------------------------------

type groupSession struct {
	client *kgo.Client
	tps    kafka.TopicPartitions
	assign *assignment
	// groupID is set when the consumer is part of a group (used for EOS).
	groupID string
}

func (s *groupSession) Assignment() kafka.Assignment { return s.assign }

func (s *groupSession) GroupMeta() (*kafka.GroupMeta, error) {
	// Encode the group ID as Meta so TransactionalProducer.SendOffsetsToTransaction can read it.
	return &kafka.GroupMeta{Meta: s.groupID}, nil
}

func (s *groupSession) TopicMeta() (kafka.TopicMeta, error) {
	return kafka.TopicMeta(s.tps), nil
}

func (s *groupSession) MarkOffset(_ context.Context, record kafka.Record, _ string) error {
	s.client.MarkCommitRecords(&kgo.Record{
		Topic:     record.Topic(),
		Partition: record.Partition(),
		Offset:    record.Offset() + 1,
	})
	return nil
}

func (s *groupSession) CommitOffset(ctx context.Context, record kafka.Record, _ string) error {
	s.client.MarkCommitRecords(&kgo.Record{
		Topic:     record.Topic(),
		Partition: record.Partition(),
		Offset:    record.Offset() + 1,
	})
	if err := s.client.CommitUncommittedOffsets(ctx); err != nil {
		return errors.Wrap(err, `franz CommitOffset failed`)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ConsumerConfig / PartitionConsumer
// ---------------------------------------------------------------------------

// ConsumerConfig wraps kafka.ConsumerConfig for the franz partition consumer.
type ConsumerConfig struct {
	*kafka.ConsumerConfig
	ExtraOpts []kgo.Opt
}

func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		ConsumerConfig: kafka.NewPartitionConsumerConfig(),
	}
}

func (c *ConsumerConfig) copy() *ConsumerConfig {
	return &ConsumerConfig{
		ConsumerConfig: c.ConsumerConfig.Copy(),
		ExtraOpts:      append([]kgo.Opt(nil), c.ExtraOpts...),
	}
}

type consumerProvider struct {
	config *ConsumerConfig
}

func NewConsumerProvider(config *ConsumerConfig) kafka.ConsumerProvider {
	return &consumerProvider{config: config}
}

func (p *consumerProvider) NewBuilder(conf *kafka.ConsumerConfig) kafka.ConsumerBuilder {
	p.config.ConsumerConfig = conf

	return func(configure func(*kafka.ConsumerConfig)) (kafka.PartitionConsumer, error) {
		cfgCopy := p.config.copy()
		configure(cfgCopy.ConsumerConfig)
		return newPartitionConsumer(cfgCopy)
	}
}

// ---------------------------------------------------------------------------
// partitionConsumer — implements kafka.PartitionConsumer
// ---------------------------------------------------------------------------

type partitionConsumer struct {
	client     *kgo.Client
	admin      *kadm.Client
	config     *ConsumerConfig
	partitions map[string]*franzPartition
	mu         sync.Mutex
}

func newPartitionConsumer(config *ConsumerConfig) (kafka.PartitionConsumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.BootstrapServers...),
		kgo.ClientID(config.Id),
	}

	if config.EOSEnabled {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	opts = append(opts, config.ExtraOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, `franz partition consumer init failed`)
	}

	return &partitionConsumer{
		client:     client,
		admin:      kadm.NewClient(client),
		config:     config,
		partitions: make(map[string]*franzPartition),
	}, nil
}

func (c *partitionConsumer) Partitions(ctx context.Context, topic string) ([]int32, error) {
	details, err := c.admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`cannot fetch partitions for topic [%s]`, topic))
	}

	td, ok := details[topic]
	if !ok {
		return nil, errors.Errorf(`topic [%s] not found in metadata`, topic)
	}
	if td.Err != nil {
		return nil, errors.Wrap(td.Err, fmt.Sprintf(`topic [%s] metadata error`, topic))
	}

	return td.Partitions.Numbers(), nil
}

func (c *partitionConsumer) ConsumeTopic(ctx context.Context, topic string, offset kafka.Offset) (map[int32]kafka.Partition, error) {
	pts, err := c.Partitions(ctx, topic)
	if err != nil {
		return nil, err
	}

	result := make(map[int32]kafka.Partition, len(pts))
	for _, pid := range pts {
		pt, err := c.ConsumePartition(ctx, topic, pid, offset)
		if err != nil {
			return nil, err
		}
		result[pid] = pt
	}
	return result, nil
}

func (c *partitionConsumer) ConsumePartition(ctx context.Context, topic string, partitionID int32, offset kafka.Offset) (kafka.Partition, error) {
	key := fmt.Sprintf(`%s-%d`, topic, partitionID)

	c.mu.Lock()
	defer c.mu.Unlock()

	if p, ok := c.partitions[key]; ok {
		return p, nil
	}

	c.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {partitionID: toKgoOffset(offset)},
	})

	startOffset, err := c.GetOffsetOldest(topic, partitionID)
	if err != nil {
		return nil, errors.Wrapf(err, `franz ConsumePartition: cannot fetch start offset for %s[%d]`, topic, partitionID)
	}

	endOffset, err := c.GetOffsetLatest(topic, partitionID)
	if err != nil {
		return nil, errors.Wrapf(err, `franz ConsumePartition: cannot fetch end offset for %s[%d]`, topic, partitionID)
	}

	p := &franzPartition{
		topic:       topic,
		partition:   partitionID,
		events:      make(chan kafka.Event, c.config.ConsumerMessageChanSize),
		client:      c.client,
		config:      c.config,
		stopCh:      make(chan struct{}),
		beginOffset: startOffset,
		endOffset:   endOffset,
	}
	c.partitions[key] = p
	go p.consumeLoop(ctx)

	return p, nil
}

func (c *partitionConsumer) OffsetValid(_ string, _ int32, _ int64) (bool, error) {
	return true, nil
}

func (c *partitionConsumer) GetOffsetLatest(topic string, partitionID int32) (int64, error) {
	listed, err := c.admin.ListEndOffsets(context.Background(), topic)
	if err != nil {
		return 0, errors.Wrap(err, `franz GetOffsetLatest failed`)
	}
	if o, ok := listed.Lookup(topic, partitionID); ok {
		return o.Offset, o.Err
	}
	return 0, errors.Errorf(`end offset not found for %s[%d]`, topic, partitionID)
}

func (c *partitionConsumer) GetOffsetOldest(topic string, partitionID int32) (int64, error) {
	listed, err := c.admin.ListStartOffsets(context.Background(), topic)
	if err != nil {
		return 0, errors.Wrap(err, `franz GetOffsetOldest failed`)
	}
	if o, ok := listed.Lookup(topic, partitionID); ok {
		return o.Offset, o.Err
	}
	return 0, errors.Errorf(`start offset not found for %s[%d]`, topic, partitionID)
}

func (c *partitionConsumer) Close() error {
	c.client.Close()
	return nil
}

// ---------------------------------------------------------------------------
// franzPartition — implements kafka.Partition
// ---------------------------------------------------------------------------

type franzPartition struct {
	topic       string
	partition   int32
	events      chan kafka.Event
	client      *kgo.Client
	config      *ConsumerConfig
	beginOffset int64
	endOffset   int64
	stopOnce    sync.Once
	stopCh      chan struct{}
}

func (p *franzPartition) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-p.stopCh:
			return
		default:
		}

		fetches := p.client.PollRecords(ctx, p.config.ConsumerMessageChanSize)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if r.Topic != p.topic || r.Partition != p.partition {
				return
			}
			rec := &record{ctx: ctx, kRec: r}
			if p.config.ContextExtractor != nil {
				rec.ctx = p.config.ContextExtractor(rec)
			}
			p.events <- rec
		})
	}
}

func (p *franzPartition) Events() <-chan kafka.Event { return p.events }
func (p *franzPartition) BeginOffset() kafka.Offset  { return kafka.Offset(p.beginOffset) }
func (p *franzPartition) EndOffset() kafka.Offset    { return kafka.Offset(p.endOffset) }

func (p *franzPartition) Close() error {
	p.stopOnce.Do(func() { close(p.stopCh) })
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func toKgoOffset(o kafka.Offset) kgo.Offset {
	switch o {
	case kafka.OffsetEarliest:
		return kgo.NewOffset().AtStart()
	case kafka.OffsetLatest:
		return kgo.NewOffset().AtEnd()
	default:
		if int64(o) >= 0 {
			return kgo.NewOffset().At(int64(o))
		}
		return kgo.NewOffset().AtEnd()
	}
}

// compile-time interface checks
var (
	_ kafka.GroupConsumerProvider = (*groupConsumerProvider)(nil)
	_ kafka.ConsumerProvider      = (*consumerProvider)(nil)
	_ kafka.GroupConsumer         = (*groupConsumer)(nil)
	_ kafka.PartitionConsumer     = (*partitionConsumer)(nil)
	_ kafka.GroupSession          = (*groupSession)(nil)
	_ kafka.PartitionClaim        = (*partitionClaim)(nil)
	_ kafka.Assignment            = (*assignment)(nil)
	_ kafka.Partition             = (*franzPartition)(nil)
)

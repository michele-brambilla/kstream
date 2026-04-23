package franz

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerConfig wraps kafka.ProducerConfig for the franz adaptor.
type ProducerConfig struct {
	*kafka.ProducerConfig
	// ExtraOpts allows passing additional kgo.Opt options.
	ExtraOpts []kgo.Opt
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		ProducerConfig: kafka.NewProducerConfig(),
	}
}

func (c *ProducerConfig) copy() *ProducerConfig {
	return &ProducerConfig{
		ProducerConfig: c.ProducerConfig.Copy(),
		ExtraOpts:      append([]kgo.Opt(nil), c.ExtraOpts...),
	}
}

// producerProvider implements kafka.ProducerProvider.
type producerProvider struct {
	config *ProducerConfig
}

func NewProducerProvider(config *ProducerConfig) kafka.ProducerProvider {
	return &producerProvider{config: config}
}

func (p *producerProvider) NewBuilder(conf *kafka.ProducerConfig) kafka.ProducerBuilder {
	p.config.ProducerConfig = conf

	return func(configure func(*kafka.ProducerConfig)) (kafka.Producer, error) {
		cfgCopy := p.config.copy()
		configure(cfgCopy.ProducerConfig)
		return newProducer(cfgCopy)
	}
}

// franzProducer wraps a kgo.Client and an optional kadm.Client for EOS.
type franzProducer struct {
	client *kgo.Client
	admin  *kadm.Client
	config *ProducerConfig
	mu     sync.Mutex
}

func newProducer(config *ProducerConfig) (*franzProducer, error) {
	opts, err := buildProducerOpts(config)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, `franz producer init failed`)
	}

	return &franzProducer{client: client, admin: kadm.NewClient(client), config: config}, nil
}

// NewProducer creates a standalone (non-builder) franz producer.
func NewProducer(config *ProducerConfig) (kafka.Producer, error) {
	return newProducer(config)
}

func buildProducerOpts(config *ProducerConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.BootstrapServers...),
		kgo.ClientID(config.Id),
	}

	switch config.Acks {
	case kafka.WaitForAll:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case kafka.WaitForLeader:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case kafka.NoResponse:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	}

	if config.Transactional.Enabled {
		opts = append(opts,
			kgo.TransactionalID(config.Transactional.Id),
			kgo.RequiredAcks(kgo.AllISRAcks()),
		)
	}

	if config.Idempotent {
		// kgo enables idempotence automatically when TransactionalID is set;
		// for explicit idempotent-only producers we use the same acks setting.
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	if config.BootstrapServers == nil || len(config.BootstrapServers) == 0 {
		return nil, errors.New(`producer: BootstrapServers must not be empty`)
	}

	opts = append(opts, config.ExtraOpts...)
	return opts, nil
}

func (p *franzProducer) NewRecord(
	ctx context.Context,
	key, value []byte,
	topic string,
	partition int32,
	timestamp time.Time,
	headers kafka.RecordHeaders,
	_ string,
) kafka.Record {
	return &producerRecord{
		ctx:       ctx,
		key:       key,
		value:     value,
		topic:     topic,
		partition: partition,
		timestamp: timestamp,
		headers:   headers,
	}
}

func (p *franzProducer) ProduceSync(ctx context.Context, record kafka.Record) (int32, int64, error) {
	kr := toKgo(record)
	results := p.client.ProduceSync(ctx, kr)
	if err := results.FirstErr(); err != nil {
		return 0, 0, errors.Wrap(err, `franz ProduceSync failed`)
	}
	return kr.Partition, kr.Offset, nil
}

func (p *franzProducer) Restart() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.client.Close()

	opts, err := buildProducerOpts(p.config)
	if err != nil {
		return err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return errors.Wrap(err, `franz producer restart failed`)
	}

	p.client = client
	return nil
}

func (p *franzProducer) Close() error {
	p.client.Close()
	return nil
}

// TransactionalProducer methods

func (p *franzProducer) ProduceAsync(ctx context.Context, record kafka.Record) error {
	kr := toKgo(record)
	p.client.Produce(ctx, kr, func(r *kgo.Record, err error) {
		if err != nil {
			p.config.Logger.Error(strings.Join([]string{`franz ProduceAsync error`, err.Error()}, `: `))
		}
	})
	return nil
}

func (p *franzProducer) InitTransactions(ctx context.Context) error {
	// franz-go initialises the transactional producer ID lazily on the first
	// BeginTransaction call. A Ping confirms broker reachability.
	return p.client.Ping(ctx)
}

func (p *franzProducer) BeginTransaction() error {
	if err := p.client.BeginTransaction(); err != nil {
		return errors.Wrap(err, `franz BeginTransaction failed`)
	}
	return nil
}

func (p *franzProducer) SendOffsetsToTransaction(ctx context.Context, offsets []kafka.ConsumerOffset, meta *kafka.GroupMeta) error {
	groupID, ok := meta.Meta.(string)
	if !ok || groupID == `` {
		return errors.New(`franz SendOffsetsToTransaction: GroupMeta.Meta must be a non-empty string groupID`)
	}

	// Build a kadm.Offsets map (next-offset = committed + 1).
	kadmOffsets := make(kadm.Offsets)
	for _, o := range offsets {
		kadmOffsets.AddOffset(o.Topic, o.Partition, o.Offset+1, -1)
	}

	// Commit offsets to the __consumer_offsets topic as part of the open transaction.
	_, err := p.admin.CommitOffsets(ctx, groupID, kadmOffsets)
	if err != nil {
		return errors.Wrap(err, `franz SendOffsetsToTransaction failed`)
	}
	return nil
}

func (p *franzProducer) CommitTransaction(ctx context.Context) error {
	if err := p.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return errors.Wrap(err, `franz CommitTransaction failed`)
	}
	return nil
}

func (p *franzProducer) AbortTransaction(ctx context.Context) error {
	if err := p.client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return errors.Wrap(err, `franz AbortTransaction failed`)
	}
	return nil
}

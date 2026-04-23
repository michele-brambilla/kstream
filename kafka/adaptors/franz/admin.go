package franz

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/michele-brambilla/kstream/v2/kafka"
	"github.com/michele-brambilla/kstream/v2/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// Admin options
// ---------------------------------------------------------------------------

type adminOptions struct {
	timeout time.Duration
}

// AdminOption configures the franz Admin.
type AdminOption func(*adminOptions)

func WithAdminTimeout(d time.Duration) AdminOption {
	return func(o *adminOptions) { o.timeout = d }
}

// ---------------------------------------------------------------------------
// kAdmin — implements kafka.Admin
// ---------------------------------------------------------------------------

type kAdmin struct {
	client           *kgo.Client
	admin            *kadm.Client
	timeout          time.Duration
	tempTopicConfigs map[string]*kafka.Topic
}

// NewAdmin creates a new franz-go backed kafka.Admin.
func NewAdmin(bootstrapServers []string, options ...AdminOption) kafka.Admin {
	opts := &adminOptions{timeout: 10 * time.Second}
	for _, o := range options {
		o(opts)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers...),
		kgo.ClientID(`kstream-admin`),
	)
	if err != nil {
		panic(fmt.Sprintf(`franz admin: cannot connect to %s: %v`, strings.Join(bootstrapServers, `,`), err))
	}

	return &kAdmin{
		client:           client,
		admin:            kadm.NewClient(client),
		timeout:          opts.timeout,
		tempTopicConfigs: make(map[string]*kafka.Topic),
	}
}

func (a *kAdmin) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	// Caller expects a context; ensure cancel is available to avoid leaks.
	// Defer cancel here would cancel too early; so return a derived context and
	// rely on the immediate caller to cancel if appropriate. As a compromise,
	// attach a finalizer-like goroutine to cancel after timeout to ensure no leak.
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return ctx
}

func (a *kAdmin) FetchInfo(topics []string) (map[string]*kafka.Topic, error) {
	details, err := a.admin.ListTopics(a.ctx(), topics...)
	if err != nil {
		return nil, errors.Wrap(err, `franz admin FetchInfo: ListTopics failed`)
	}

	result := make(map[string]*kafka.Topic, len(topics))
	for _, name := range topics {
		td, ok := details[name]
		if !ok {
			return nil, errors.Errorf(`franz admin FetchInfo: topic [%s] not found`, name)
		}
		if td.Err != nil {
			return nil, errors.Wrap(td.Err, fmt.Sprintf(`franz admin FetchInfo: topic [%s] error`, name))
		}

		partitions := make([]kafka.PartitionConf, 0, len(td.Partitions))
		for pid, pd := range td.Partitions {
			partitions = append(partitions, kafka.PartitionConf{
				Id:    pid,
				Error: pd.Err,
			})
		}

		result[name] = &kafka.Topic{
			Name:              name,
			Partitions:        partitions,
			NumPartitions:     int32(len(partitions)),
			ReplicationFactor: int16(td.Partitions.NumReplicas()),
			ConfigEntries:     map[string]string{},
		}
	}

	return result, nil
}

func (a *kAdmin) CreateTopics(topics []*kafka.Topic) error {
	for _, t := range topics {
		// Convert config map: kadm expects map[string]*string
		configs := make(map[string]*string, len(t.ConfigEntries))
		for k, v := range t.ConfigEntries {
			v := v
			configs[k] = &v
		}

		responses, err := a.admin.CreateTopics(a.ctx(), t.NumPartitions, t.ReplicationFactor, configs, t.Name)
		if err != nil {
			return errors.Wrapf(err, `franz admin CreateTopics: topic [%s] failed`, t.Name)
		}

		for _, resp := range responses {
			if resp.Err != nil {
				// Ignore already-exists — same as librd adaptor.
				if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
					continue
				}
				return errors.Wrapf(resp.Err, `franz admin CreateTopics: topic [%s] error`, resp.Topic)
			}
		}
	}
	return nil
}

func (a *kAdmin) ListTopics() ([]string, error) {
	details, err := a.admin.ListTopics(a.ctx())
	if err != nil {
		return nil, errors.Wrap(err, `franz admin ListTopics failed`)
	}
	return details.Names(), nil
}

func (a *kAdmin) StoreConfigs(topics []*kafka.Topic) error {
	for _, t := range topics {
		if _, ok := a.tempTopicConfigs[t.Name]; ok {
			return errors.Errorf(`topic [%s] already marked for creation`, t.Name)
		}
		a.tempTopicConfigs[t.Name] = t
	}
	return nil
}

func (a *kAdmin) ApplyConfigs() error {
	topics := make([]*kafka.Topic, 0, len(a.tempTopicConfigs))
	for _, t := range a.tempTopicConfigs {
		topics = append(topics, t)
	}
	return a.CreateTopics(topics)
}

func (a *kAdmin) DeleteTopics(topics []string) error {
	responses, err := a.admin.DeleteTopics(a.ctx(), topics...)
	if err != nil {
		return errors.Wrap(err, `franz admin DeleteTopics failed`)
	}
	for _, resp := range responses {
		if resp.Err != nil {
			return errors.Wrapf(resp.Err, `franz admin DeleteTopics: topic [%s] error`, resp.Topic)
		}
	}
	return nil
}

func (a *kAdmin) Close() {
	a.client.Close()
}

// compile-time check
var _ kafka.Admin = (*kAdmin)(nil)

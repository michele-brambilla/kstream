package streams

import (
	"fmt"
	"github.com/michele-brambilla/kstream/v2/kafka"
	"github.com/michele-brambilla/kstream/v2/pkg/errors"
	"github.com/michele-brambilla/kstream/v2/streams/tasks"
	"github.com/michele-brambilla/kstream/v2/streams/topology"
	"github.com/tryfix/log"
	"sync"
)

type streamConsumer struct {
	consumerCount int
	groupConsumer kafka.GroupConsumerBuilder
	logger        log.Logger
	ctx           topology.BuilderContext

	taskManager tasks.TaskManager

	consumers []*streamConsumerInstance
	// running is closed once all instances have successfully subscribed
	running chan struct{}
	// stop is closed by Stop() to signal Run() to exit
	stop chan struct{}
	// done is closed by Run() when it is about to return
	done chan struct{}
}

func (r *streamConsumer) Init(_ topology.Topology) error { return nil }

func (r *streamConsumer) Run(topologyBuilder topology.Topology) error {
	r.logger.Info(`StreamConsumer starting...`)

	// Get topic meta
	meta, err := r.ctx.Admin().FetchInfo(topologyBuilder.StreamTopologies().SourceTopics())
	if err != nil {
		return errors.Wrap(err, `topics meta fetch failed`)
	}

	var tps []kafka.TopicPartition
	for _, tp := range meta {
		for _, partition := range tp.Partitions {
			tps = append(tps, kafka.TopicPartition{
				Topic:     tp.Name,
				Partition: partition.Id,
			})
		}
	}

	// Generate task list
	generation := new(tasks.Generator).Generate(tps, topologyBuilder)
	r.logger.Info(fmt.Sprintf("Task list generated -> \n%s", generation.Mappings()))

	wg := &sync.WaitGroup{}
	for i := 1; i <= r.consumerCount; i++ {
		consumerID := fmt.Sprintf(`StreamConsumer#%d`, i)
		logger := r.logger.NewLog(log.Prefixed(consumerID))
		consumer, err := r.groupConsumer(func(config *kafka.GroupConsumerConfig) {
			config.Logger = logger
			config.Id = consumerID
		})
		if err != nil {
			return err
		}

		instance := &streamConsumerInstance{
			id:              consumerID,
			topologyBuilder: topologyBuilder,
			ctx:             r.ctx,
			generation:      generation,
			logger:          logger,
			taskManager:     r.taskManager,
			consumer:        consumer,
		}

		r.consumers = append(r.consumers, instance)

		wg.Add(1)
		go func(instance *streamConsumerInstance) {
			if err := instance.Subscribe(); err != nil {
				panic(err)
			}
			wg.Done()
		}(instance)
	}

	wg.Wait()

	// signal that subscription is complete
	if r.running == nil {
		r.running = make(chan struct{})
	}
	close(r.running)

	// initialize stop/done if not already
	if r.stop == nil {
		r.stop = make(chan struct{})
	}
	if r.done == nil {
		r.done = make(chan struct{})
	}

	// block until Stop() signals exit
	<-r.stop

	// signal Stop() that Run() is exiting
	close(r.done)

	return nil
}

func (r *streamConsumer) Ready() error {
	return nil
}

func (r *streamConsumer) Stop() error {
	r.logger.Info(`StreamConsumer stopping...`)
	defer r.logger.Info(`StreamConsumer stopped`)

	for _, instance := range r.consumers {
		go func(i *streamConsumerInstance) {
			if err := i.Unsubscribe(); err != nil {
				r.logger.Error(err)
			}
		}(instance)
	}

	// wait until subscriptions are active
	if r.running != nil {
		<-r.running
	}

	// signal Run() to finish and wait for it
	if r.stop != nil {
		close(r.stop)
	}
	if r.done != nil {
		<-r.done
	}

	return nil
}

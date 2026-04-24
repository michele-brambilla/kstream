package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/michele-brambilla/kstream/v2/kafka/adaptors"

	"github.com/michele-brambilla/kstream/v2/kafka"
	"github.com/michele-brambilla/kstream/v2/streams"
	"github.com/michele-brambilla/kstream/v2/streams/encoding"
	"github.com/tryfix/log"

	"net/http"
	_ "net/http/pprof"
)

var bootstrapServers = flag.String(`bootstrap-servers`, `192.168.0.101:9092`,
	`A comma seperated list Kafka Bootstrap Servers`)

const TopicNumbers = `numbers`

func main() {
	go func() {
		log.Info(http.ListenAndServe("localhost:6060", nil))
	}()

	flag.Parse()

	config := streams.NewStreamBuilderConfig()
	config.Logger = log.StdLogger.NewLog(log.WithLevel(log.TRACE))
	config.BootstrapServers = strings.Split(*bootstrapServers, `,`)
	config.ApplicationId = `kstream-branching`
	config.Consumer.Offsets.Initial = kafka.OffsetEarliest

	// seed(config.Logger)

	builder := streams.NewStreamBuilder(config)
	buildTopology(builder)

	topology, err := builder.Build()
	if err != nil {
		panic(err)
	}

	println("Topology - \n", topology.Describe())

	runner := builder.NewRunner()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		if err := runner.Stop(); err != nil {
			println(err)
		}
	}()

	if err := runner.Run(topology); err != nil {
		panic(err)
	}
}

func buildTopology(builder *streams.StreamBuilder) {
	stream := builder.KStream(TopicNumbers, encoding.NoopEncoder{}, encoding.IntEncoder{})
	splitted := stream.Split()
	splitted.New(`odd`, func(ctx context.Context, key interface{}, val interface{}) (bool, error) {
		return val.(int)%2 != 0, nil
	})
	splitted.New(`even`, func(ctx context.Context, key interface{}, val interface{}) (bool, error) {
		return val.(int)%2 == 0, nil
	})

	splitted.Branch(`odd`).Each(func(ctx context.Context, key, value interface{}) {
		println(`Odd number:`, value.(int))
	})

	splitted.Branch(`even`).Each(func(ctx context.Context, key, value interface{}) {
		println(`Even number:`, value.(int))
	})
}

func seed(logger log.Logger) {
	providers := adaptors.ProvidersFromEnv(strings.Split(*bootstrapServers, `,`))

	// Build a transactional producer via the provider's builder
	producerBuilder := providers.Producer.NewBuilder(&kafka.ProducerConfig{})
	producer, err := producerBuilder(func(pc *kafka.ProducerConfig) {
		pc.BootstrapServers = strings.Split(*bootstrapServers, `,`)
		pc.Transactional.Enabled = true
		pc.Transactional.Id = `words-producer`
	})
	if err != nil {
		panic(err)
	}

	txProducer := producer.(kafka.TransactionalProducer)

	if err := txProducer.InitTransactions(context.Background()); err != nil {
		panic(err)
	}

	if err := txProducer.BeginTransaction(); err != nil {
		panic(err)
	}

	for i := 1; i <= 100; i++ {
		record := producer.NewRecord(
			context.Background(),
			nil,
			[]byte(fmt.Sprint(i)),
			TopicNumbers,
			kafka.PartitionAny,
			time.Now(),
			nil,
			``,
		)

		err := txProducer.ProduceAsync(context.Background(), record)
		if err != nil {
			panic(err)
		}

		logger.Debug(`message produced to`, TopicNumbers)
	}

	if err := txProducer.CommitTransaction(context.Background()); err != nil {
		panic(err)
	}

	logger.Info(`Test records produced`)
}

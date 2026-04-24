package adaptors

import (
	"log"
	"os"

	"github.com/michele-brambilla/kstream/v2/kafka/adaptors/franz"

	"github.com/michele-brambilla/kstream/v2/kafka"
	"github.com/michele-brambilla/kstream/v2/kafka/adaptors/librd"
)

const envKey = "KSTREAM_CLIENT"

// ClientType selects the Kafka client adaptor.
type ClientType string

const (
	ClientFranz ClientType = "franz"
	ClientLibrd ClientType = "librd"
)

// Providers bundles the three provider types needed to build a kstream instance.
type Providers struct {
	Producer      kafka.ProducerProvider
	GroupConsumer kafka.GroupConsumerProvider
	Consumer      kafka.ConsumerProvider
}

// ProvidersFromEnv returns Providers based on the KSTREAM_CLIENT environment
// variable. Defaults to librd when unset or unknown.
//
//	KSTREAM_CLIENT=franz   → franz-go adaptor
//	KSTREAM_CLIENT=librd   → librdkafka adaptor (default)
func ProvidersFromEnv(bootstrapServers []string) Providers {
	switch ClientType(os.Getenv(envKey)) {
	case ClientFranz:
		log.Print("Using franz-go adaptor")
		return franzProviders(bootstrapServers)
	default:
		log.Print("Using librd adaptor")
		return librdProviders(bootstrapServers)
	}
}

// ProvidersFor returns Providers for the given ClientType explicitly.
func ProvidersFor(ct ClientType, bootstrapServers []string) Providers {
	switch ct {
	case ClientFranz:
		return franzProviders(bootstrapServers)
	default:
		return franzProviders(bootstrapServers)
		// return librdProviders(bootstrapServers)
	}
}

func franzProviders(bootstrapServers []string) Providers {
	prodConf := franz.NewProducerConfig()
	prodConf.BootstrapServers = bootstrapServers

	gcConf := franz.NewGroupConsumerConfig()
	gcConf.BootstrapServers = bootstrapServers

	cConf := franz.NewConsumerConfig()
	cConf.BootstrapServers = bootstrapServers

	return Providers{
		Producer:      franz.NewProducerProvider(prodConf),
		GroupConsumer: franz.NewGroupConsumerProvider(gcConf),
		Consumer:      franz.NewConsumerProvider(cConf),
	}
}

func librdProviders(bootstrapServers []string) Providers {
	prodConf := librd.NewProducerConfig()
	prodConf.BootstrapServers = bootstrapServers

	gcConf := librd.NewGroupConsumerConfig()
	gcConf.BootstrapServers = bootstrapServers

	cConf := librd.NewConsumerConfig()
	cConf.BootstrapServers = bootstrapServers

	return Providers{
		Producer:      librd.NewProducerProvider(prodConf),
		GroupConsumer: librd.NewGroupConsumerProvider(gcConf),
		Consumer:      librd.NewConsumerProvider(cConf),
	}
}

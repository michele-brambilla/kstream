package librd

import (
	"testing"

	"github.com/michele-brambilla/kstream/v2/kafka"
)

func TestNewProducer_WithMissingBootstrapServers_ReturnsError(t *testing.T) {
	pc := NewProducerConfig()
	pc.ProducerConfig = &kafka.ProducerConfig{}
	_, err := NewProducer(pc)
	if err == nil {
		t.Fatalf("expected error when bootstrap servers missing")
	}
}

func TestNewProducer_WithNilLoggerAndMetrics_DoesNotPanic(t *testing.T) {
	pc := NewProducerConfig()
	pc.ProducerConfig = &kafka.ProducerConfig{
		BootstrapServers: []string{"localhost:9092"},
	}
	pc.ProducerConfig.Logger = nil
	pc.ProducerConfig.MetricsReporter = nil
	p, err := NewProducer(pc)
	if err != nil {
		t.Fatalf("expected NewProducer to handle nil logger/metrics gracefully: %v", err)
	}
	// Close if created
	_ = p.Close()
}

func TestProducerConfig_validate_TransactionalRequiresID(t *testing.T) {
	pc := NewProducerConfig()
	pc.ProducerConfig = &kafka.ProducerConfig{
		BootstrapServers: []string{"localhost:9092"},
	}
	pc.Transactional.Enabled = true
	pc.Transactional.Id = ""
	if err := pc.validate(); err == nil {
		t.Fatalf("expected error when transactional enabled but id missing")
	}
}

func TestProducer_Builder_ProducesErrorOnInvalidConfig(t *testing.T) {
	prov := NewProducerProvider(NewProducerConfig())
	builder := prov.NewBuilder(&kafka.ProducerConfig{})
	// builder should return an error due to missing bootstrap servers
	_, err := builder(func(pc *kafka.ProducerConfig) {})
	if err == nil {
		t.Fatalf("expected builder to return error for invalid config")
	}
}

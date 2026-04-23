package franz

import (
	"os"
	"testing"
)

func TestProvidersFromEnv_defaultsToLibrd(t *testing.T) {
	os.Unsetenv(envKey)
	p := ProvidersFromEnv([]string{"localhost:9092"})
	if p.Producer == nil {
		t.Error("expected non-nil Producer provider")
	}
	if p.GroupConsumer == nil {
		t.Error("expected non-nil GroupConsumer provider")
	}
	if p.Consumer == nil {
		t.Error("expected non-nil Consumer provider")
	}
}

func TestProvidersFromEnv_franzWhenEnvSet(t *testing.T) {
	t.Setenv(envKey, "franz")
	p := ProvidersFromEnv([]string{"localhost:9092"})
	if p.Producer == nil {
		t.Error("expected non-nil Producer provider")
	}
	if p.GroupConsumer == nil {
		t.Error("expected non-nil GroupConsumer provider")
	}
	if p.Consumer == nil {
		t.Error("expected non-nil Consumer provider")
	}
}

func TestProvidersFor_franz(t *testing.T) {
	p := ProvidersFor(ClientFranz, []string{"localhost:9092"})
	if p.Producer == nil || p.GroupConsumer == nil || p.Consumer == nil {
		t.Error("expected all providers non-nil for ClientFranz")
	}
}

func TestProvidersFor_librd(t *testing.T) {
	p := ProvidersFor(ClientLibrd, []string{"localhost:9092"})
	if p.Producer == nil || p.GroupConsumer == nil || p.Consumer == nil {
		t.Error("expected all providers non-nil for ClientLibrd")
	}
}

func TestProvidersFor_unknownDefaultsToLibrd(t *testing.T) {
	p := ProvidersFor(ClientType("unknown"), []string{"localhost:9092"})
	if p.Producer == nil || p.GroupConsumer == nil || p.Consumer == nil {
		t.Error("expected all providers non-nil for unknown type (default librd)")
	}
}

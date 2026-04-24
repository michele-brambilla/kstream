
## kafka/adaptors/factory.go
Comment on lines +51 to +53

`ProvidersFor` falls through to `franzProviders` for the default case, so `ClientLibrd` (and any unknown value) incorrectly returns the franz-go providers. This makes explicit selection of librd impossible and diverges from `ProvidersFromEnv`’s documented default. Return `librdProviders` in the default branch (and ideally add a case for `ClientLibrd`).
```suggestion
	case ClientLibrd:
		return librdProviders(bootstrapServers)
	default:
		return librdProviders(bootstrapServers)
```

## streams/stream_consumer.go
Comment on lines +90 to +109

`Run` closes `r.running` and only then initializes `r.stop`/`r.done`, but `Stop()` only closes `r.stop`/waits on `r.done` if those channels are already non-nil. If `Stop()` is called before `Run()` reaches the stop/done initialization, `Stop()` returns without signalling, and `Run()` can block forever on `<-r.stop`. Initialize `running/stop/done` eagerly (e.g., in constructor/Init) or guard with `sync.Once` and ensure `Stop()` always signals `Run()` regardless of call ordering.

## streams/stream_consumer.go
Comment on lines 55 to +105

This `Run` implementation waits on `wg.Wait()` for `instance.Subscribe()` to return before signalling `running`. With the librd adaptor, `GroupConsumer.Subscribe` is a blocking call that returns only when consumption stops; this means `running` is never closed until shutdown. Combined with the new `stop/done` handshake, `Stop()` can return without ever unblocking `Run()`, leaving the runner hung. Consider restoring the previous lifecycle (Run blocks until Subscribe returns and Stop only triggers unsubscribe), or change adaptors (including librd/franz) to a consistent contract and signal readiness separately from the Subscribe return path.

## streams/tasks/ktask.go
Comment on lines +283 to +285

Printing to stdout on every processed record (`fmt.Printf`) will severely impact throughput and flood logs in production. Prefer existing structured logging (at Trace/Debug level) and/or a metric counter, and keep per-record logging opt-in.

## streams/stream_builder.go
Comment on lines +287 to +288

`fmt.Printf` in library code introduces noisy stdout output and bypasses the configured logger. Use `b.config.Logger` (or remove this entirely) so users can control output level/format.

## README.md

Comment on lines +78 to +90
The README’s programmatic selection example imports `kafka/adaptors/franz` but calls `ProvidersFor` / `ProvidersFromEnv` and references `ClientFranz`, which are defined in `kafka/adaptors` (factory.go), not in the `franz` package. As written, the example won’t compile. Update the import and identifiers to match the actual factory package/API.

Comment on lines +4 to +6
The Releases and LICENSE badge image URLs still reference gmbyapa/kstream, while links point to michele-brambilla/kstream. This will render incorrect badges for the forked repo. Update the badge image URLs to the new GitHub org/repo to keep README metadata accurate.

## go.mod
Comment on lines +1 to 4

This `replace` directive points the module to itself at a pseudo-version. That can cause confusing module resolution (and may force fetching the remote version instead of using the local main module), undermining local development and CI reproducibility. If the goal is compatibility for the old import path, replace `github.com/gmbyapa/kstream/v2` -> `github.com/michele-brambilla/kstream/v2` instead; otherwise remove the self-replace.

## kafka/adaptors/franz/consumer.go
Comment on lines +73 to +180

`drainErrors` continuously reads from `g.errs` and discards errors, which means consumers of `Errors()` will miss errors entirely. It also leaks a goroutine because `errs` is never closed. Instead, remove `drainErrors` and make error sends non-blocking (or log internally) so writers don’t deadlock when nobody is reading, and close `errs` on `Unsubscribe()`.

Comment on lines +217 to +222
There are multiple unconditional `fmt.Printf` debug prints inside the consumer hot path (fetch summary, OnPartitionAssigned messages) and an empty-poll is treated as an error (`g.errs <- "franz: no records in this poll"`). Empty polls are normal and should not be surfaced as errors. Remove stdout prints and use the configured logger at an appropriate level, and don’t emit an error when `len(byPartition) == 0`.

Comment on lines +275 to +280
`consumeLoop` calls `handler.OnPartitionAssigned` and `handler.OnPartitionRevoked` for every poll batch (not just on actual group rebalances). This can repeatedly reinitialize/tear down tasks and is not equivalent to librdkafka’s rebalance semantics. `OnPartitionAssigned`/`OnPartitionRevoked` should be invoked only from the rebalance callbacks (`kgo.OnPartitionsAssigned` / `kgo.OnPartitionsRevoked`), while `Consume` should run for records within the steady-state assignment.
```suggestion

```

## kafka/adaptors/franz/producer.go
`SendOffsetsToTransaction` adds `+1` to `ConsumerOffset.Offset` when building `kadmOffsets`. In this codebase, `ConsumerOffset.Offset` is already stored as the *next* offset to commit (e.g., `record.Offset()+1` in `commitBuffer.Add`). Adding another `+1` will commit too far and can skip messages. Use `o.Offset` as-is when committing.
```suggestion
		kadmOffsets.AddOffset(o.Topic, o.Partition, o.Offset, -1)
```

## kafka/adaptors/franz/admin.go
Comment on lines +65 to +79
`ctx()` spawns a goroutine per admin call and never returns the `cancel` function to the caller, creating avoidable overhead and making cancellation semantics unclear. Prefer returning `(context.Context, context.CancelFunc)` and `defer cancel()` in each public method, or inline `context.WithTimeout` in each method with a `defer cancel()`.
```suggestion
func (a *kAdmin) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), a.timeout)
}

func (a *kAdmin) FetchInfo(topics []string) (map[string]*kafka.Topic, error) {
	ctx, cancel := a.ctx()
	defer cancel()

	details, err := a.admin.ListTopics(ctx, topics...)
```

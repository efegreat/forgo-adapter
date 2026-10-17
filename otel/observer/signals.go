package observer

import (
	"context"
	"net"
	"sync"

	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/worker"
	"github.com/efegreat/forgo/transport"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// messageKey is the labels of a message: its subscriber and its operation's name, the handler's or
// not_found.
type messageKey struct {
	subscriber, name string
}

func (k messageKey) attrs() []attribute.KeyValue {
	return []attribute.KeyValue{attrSubscriber.String(k.subscriber), attrOperationName.String(k.name)}
}

func buildMessage(k messageKey) []metric.RecordOption {
	return []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(k.attrs()...))}
}

// settlementKey is the labels of forgo.message.settled.
type settlementKey struct {
	messageKey
	kind              worker.SettlementKind
	duplicate, failed bool
}

func buildSettlement(k settlementKey) []metric.AddOption {
	attrs := append(k.attrs(),
		attrSettlementKind.String(k.kind.String()),
		attrSettlementDuplicate.Bool(k.duplicate),
		attrSettlementFailed.Bool(k.failed),
	)
	return []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(attrs...))}
}

// Settled counts the settlement of a delivery.
func (o *observer) Settled(ctx *operation.Context, s worker.Settlement) {
	k := settlementKey{messageKey: messageKey{name: ctx.Name}, kind: s.Kind, duplicate: s.Duplicate, failed: s.Err != nil}
	if st, ok := ctx.State.(*worker.State); ok {
		k.subscriber = st.Subscriber
	}
	o.settled.Add(ctx, 1, o.settlements.get(k, buildSettlement)...)
}

// Relayed records the duration of an outbox relay batch, and counts its published and unpublished messages.
func (o *observer) Relayed(ctx context.Context, b worker.RelayBatch) {
	opts := o.batchOK
	if b.Err != nil {
		opts = o.batchFailed
	}
	o.batch.Record(ctx, b.Elapsed.Seconds(), opts...)
	if b.Published > 0 {
		o.published.Add(ctx, int64(b.Published))
	}
	if failed := b.Claimed - b.Published; failed > 0 {
		o.unpublished.Add(ctx, int64(failed))
	}
}

// LockHeld counts an operation skipped or retried for a lock another holder had, labelled by the operation,
// never by key, which can hold a message ID.
func (o *observer) LockHeld(ctx context.Context, _ string) {
	o.lockHeld.Add(ctx, 1, o.lockOptions(ctx)...)
}

// LockFailed counts an operation whose lock could not be acquired.
func (o *observer) LockFailed(ctx context.Context, _ string, _ error) {
	o.lockFailed.Add(ctx, 1, o.lockOptions(ctx)...)
}

// lockOptions returns the labels of the operation of ctx, none outside an operation.
func (o *observer) lockOptions(ctx context.Context) []metric.AddOption {
	op, ok := operation.FromCtx(ctx)
	if !ok {
		return nil
	}
	return o.locks.get(opKey{op.Type, op.Name}, buildActive)
}

// grpcServerName is the server name of forgo's gRPC server, grpc.ServerNameGRPC, which this package does not
// import, so that applications without a gRPC server do not link it.
const grpcServerName = "grpc"

// ConnObserver returns the connection observer of server, labelled with its name and address. It is an
// transport.IConnStateObserver, and also an transport.IConnIOObserver with Config.ConnIO.
func (o *observer) ConnObserver(server transport.Server) transport.IConnObserver {
	attrs := []attribute.KeyValue{attrServerName.String(server.Name), attrServerAddress.String(server.Addr)}
	options := func(extra ...attribute.KeyValue) []metric.AddOption {
		return []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(append(append([]attribute.KeyValue(nil), attrs...), extra...)...))}
	}
	c := &conns{o: o, server: options(), states: make(map[net.Conn]transport.ConnState)}
	for state := transport.ConnNew; state < transport.ConnClosed; state++ {
		label := state.String()
		// gRPC reports no state between new and closed: its connections are open.
		if state == transport.ConnNew && server.Name == grpcServerName {
			label = "open"
		}
		c.byState[state] = options(attrConnState.String(label))
	}
	if o.ioErrors == nil {
		return c
	}
	return &ioConns{conns: c, read: options(attrConnDirection.String("read")), write: options(attrConnDirection.String("write"))}
}

// conns observes the connections of one server.
type conns struct {
	o *observer
	// server labels the server's counters, and byState its open connections in each state but ConnClosed.
	server  []metric.AddOption
	byState [transport.ConnClosed][]metric.AddOption

	mu sync.Mutex
	// states holds the last state of every open connection.
	states map[net.Conn]transport.ConnState
}

func (c *conns) OnAcceptError(error) {
	c.o.acceptErrors.Add(context.Background(), 1, c.server...)
}

// OnConnState moves the connection to its new state in forgo.conn.open, and counts the new ones.
func (c *conns) OnConnState(conn net.Conn, state transport.ConnState) {
	if state > transport.ConnClosed {
		return
	}
	c.mu.Lock()
	prev, known := c.states[conn]
	if state == transport.ConnClosed {
		delete(c.states, conn)
	} else {
		c.states[conn] = state
	}
	c.mu.Unlock()

	ctx := context.Background()
	if known {
		c.o.open.Add(ctx, -1, c.byState[prev]...)
	}
	if state == transport.ConnNew {
		c.o.opened.Add(ctx, 1, c.server...)
	}
	if state != transport.ConnClosed {
		c.o.open.Add(ctx, 1, c.byState[state]...)
	}
}

// ioConns is conns that also counts read and write errors.
type ioConns struct {
	*conns
	read, write []metric.AddOption
}

func (c *ioConns) OnReadError(net.Conn, error) {
	c.o.ioErrors.Add(context.Background(), 1, c.read...)
}

func (c *ioConns) OnWriteError(net.Conn, error) {
	c.o.ioErrors.Add(context.Background(), 1, c.write...)
}

var (
	_ operation.IObserver            = (*observer)(nil)
	_ worker.IDeliveryObserver       = (*observer)(nil)
	_ worker.IRelayObserver          = (*observer)(nil)
	_ operation.ILockObserver        = (*observer)(nil)
	_ transport.IConnObserverFactory = (*observer)(nil)
	_ transport.IConnStateObserver   = (*conns)(nil)
	_ transport.IConnIOObserver      = (*ioConns)(nil)
	_ transport.IConnStateObserver   = (*ioConns)(nil)
)

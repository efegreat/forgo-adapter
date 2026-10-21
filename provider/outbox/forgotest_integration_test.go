package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/server/http"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/internal/sqltest"
	"github.com/efegreat/forgo-adapter/provider/inbox"
	"github.com/efegreat/forgo-adapter/provider/outbox"
	"github.com/efegreat/forgo-adapter/provider/sqltx"
)

var (
	errDown    = errors.New("shipping down")
	errInvalid = errors.New("invalid order")
)

type order struct {
	OrderID string `json:"order_id" validate:"required"`
}

// saver saves messages to the outbox, in the transaction of ctx.
type saver interface {
	Save(ctx context.Context, msg worker.Message) error
}

// shop saves an order_created message to the outbox in the transaction of each order it places, and ships
// each order whose message it handles.
type shop struct {
	d      sqltest.Dialect
	db     *sql.DB
	outbox saver

	mu    sync.Mutex
	saved []worker.Message
}

// Place saves the order's message, then fails the order "invalid", which rolls the message back.
func (s *shop) Place(ctx context.Context, req order) (order, error) {
	err := sqltx.Run(ctx, s.db, func(ctx context.Context) error {
		msg, err := s.save(ctx, req.OrderID)
		if err != nil {
			return err
		}
		if req.OrderID == "invalid" {
			return errInvalid
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.saved = append(s.saved, msg)
		return nil
	})
	return req, err
}

// save writes the order_created message of orderID to the outbox, in the transaction of ctx.
func (s *shop) save(ctx context.Context, orderID string) (worker.Message, error) {
	msg := worker.NewMessage("order_created", orderID, "orders", fmt.Appendf(nil, `{"order_id":%q}`, orderID))
	msg.Headers.Set("tenant", "acme")
	return msg, s.outbox.Save(ctx, msg)
}

// Ship records the shipment of the order, then fails the order "down", which rolls the shipment back.
func (s *shop) Ship(ctx context.Context, e order) error {
	if _, err := sqltx.From(ctx, s.db).ExecContext(ctx,
		`INSERT INTO shipments (order_id) VALUES (`+s.d.Placeholder(1)+`)`, e.OrderID); err != nil {
		return err
	}
	if e.OrderID == "down" {
		return errDown
	}
	return nil
}

// TestIntegrationForgotest runs an app through forgo's forgotest on the SQL outbox, inbox and transactor:
// the contract test of forgo's REFERENCE.md (Testing), where one env's endpoint saves a message that
// Env.Outbox reads and another env's consumer handles through Env.Deliver. It runs on a database of its own,
// since it uses the inbox's table, which the tests of provider/inbox reset in parallel.
func TestIntegrationForgotest(t *testing.T) {
	for _, d := range sqltest.Dialects {
		t.Run(d.Name, func(t *testing.T) {
			db := d.OpenDatabase(t, "forgotest")
			d.Reset(t, db, "outbox_events")
			d.Reset(t, db, "inbox_events")
			exec(t, db, `DROP TABLE IF EXISTS shipments`)
			exec(t, db, `CREATE TABLE shipments (order_id VARCHAR(255) PRIMARY KEY)`)
			ctx := context.Background()

			var box interface {
				worker.OutboxStorage
				saver
			} = outbox.NewMySQL(db)
			var marks worker.InboxStorage = inbox.NewMySQL(db)
			if d == sqltest.Postgres {
				box, marks = outbox.NewPostgres(db), inbox.NewPostgres(db)
			}
			s := &shop{d: d, db: db, outbox: box}
			// newApp builds the app with servers, as the function that main calls would.
			newApp := func(servers ...forgo.ServerFactory) *forgo.App {
				return forgo.NewApp(
					forgo.WithName("shop"),
					forgo.WithResources(forgotest.Broker("rabbitmq")),
					forgo.WithProviders(
						func() *sql.DB { return db },
						func() worker.OutboxStorage { return box },
						func() worker.InboxStorage { return marks },
						sqltx.NewTransactor,
						func() *shop { return s },
					),
					forgo.WithServers(servers...),
				)
			}
			api := newEnv(t, newApp(http.Config{Routes: []http.RouteConfig{
				{Name: "place_order", Method: "POST", Path: "/orders", Handler: http.HandlerFor((*shop).Place)},
			}}))
			defer api.Close()
			consumer := newEnv(t, newApp(worker.ConsumerConfig{Subscribers: []worker.SubscriberConfig{{
				Name:     "shipping",
				Handlers: []worker.HandlerConfig{{Name: "order_created", Handler: worker.HandlerFor((*shop).Ship)}},
			}}}))
			defer consumer.Close()
			h, err := api.Handler(http.ServerNameHTTP)
			if err != nil {
				t.Fatal(err)
			}

			// readOne places orderID and returns the one message Outbox reads after it.
			readOne := func(orderID string) worker.Message {
				t.Helper()
				if code := place(h, orderID); code != nethttp.StatusOK {
					t.Fatalf("POST /orders %s: %d", orderID, code)
				}
				msgs, err := api.Outbox(ctx)
				if err != nil || len(msgs) != 1 {
					t.Fatalf("Outbox after order %s: %v, %v", orderID, msgs, err)
				}
				return msgs[0]
			}
			deliver := func(msg worker.Message, want forgotest.Settlement, attempt int, wantErr error) {
				t.Helper()
				got, err := consumer.Deliver(ctx, "shipping", msg)
				if err != nil || got.Settlement != want || got.Attempt != attempt || !errors.Is(got.Err, wantErr) {
					t.Fatalf("Deliver %s: %+v (%s), %v; want %s, attempt %d, error %v",
						msg.Key, got, got.Settlement, err, want, attempt, wantErr)
				}
			}

			// The endpoint saved the message as the relay reads it, through the database.
			msg := readOne("a")
			checkRead(t, msg, s.saved[0])
			if msgs, err := api.Outbox(ctx); err != nil || len(msgs) != 0 {
				t.Fatalf("Outbox read the same message again: %v, %v", msgs, err)
			}
			if n := count(t, db, `SELECT COUNT(*) FROM outbox_events WHERE processing_id LIKE 'forgotest-%' AND processed_at IS NULL`); n != 1 {
				t.Fatalf("%d rows claimed by forgotest, want the message read, claimed and not processed", n)
			}

			// The handler ships it, in the transaction that completes it in the inbox, and once only.
			deliver(msg, forgotest.Completed, 0, nil)
			deliver(msg, forgotest.Completed, 0, nil)
			if st, ok, err := marks.Status(ctx, msg.ID, "shipping"); err != nil || !ok || st != worker.InboxCompleted {
				t.Fatalf("inbox status %v, %t, %v", st, ok, err)
			}
			if n := count(t, db, `SELECT COUNT(*) FROM shipments WHERE order_id = 'a'`); n != 1 {
				t.Fatalf("%d shipments of a, want 1", n)
			}

			// A handler that fails rolls its shipment back, and its attempts count up to MaxAttempt, 3.
			down := readOne("down")
			deliver(down, forgotest.Retried, 1, errDown)
			deliver(down, forgotest.Retried, 2, errDown)
			deliver(down, forgotest.Rejected, 0, errDown)
			deliver(down, forgotest.Rejected, 0, nil) // failed already: the handler does not run
			if st, ok, err := marks.Status(ctx, down.ID, "shipping"); err != nil || !ok || st != worker.InboxFailed {
				t.Fatalf("inbox status %v, %t, %v", st, ok, err)
			}
			if n := count(t, db, `SELECT COUNT(*) FROM shipments WHERE order_id = 'down'`); n != 0 {
				t.Fatalf("%d shipments of down, want them rolled back", n)
			}

			// An endpoint whose transaction rolls back saved nothing.
			if code := place(h, "invalid"); code == nethttp.StatusOK {
				t.Fatal("POST /orders invalid succeeded")
			}
			if msgs, err := api.Outbox(ctx); err != nil || len(msgs) != 0 {
				t.Fatalf("Outbox after a rolled back order: %v, %v", msgs, err)
			}

			// Calls at the same time return each message once between them, through SKIP LOCKED, over more
			// than one claim each.
			var want []string
			for i := range 500 {
				msg, err := s.save(ctx, fmt.Sprint(i))
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, msg.ID)
			}
			var mu sync.Mutex
			var read []string
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					msgs, err := api.Outbox(ctx)
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					for _, m := range msgs {
						read = append(read, m.ID)
					}
				})
			}
			wg.Wait()
			slices.Sort(read)
			if !slices.Equal(read, want) {
				t.Fatalf("the calls returned %d messages, want each of the %d saved once", len(read), len(want))
			}
		})
	}
}

// newEnv builds an env of app that logs nowhere.
func newEnv(t *testing.T, app *forgo.App) *forgotest.Env {
	t.Helper()
	env, err := forgotest.New(app, forgotest.WithLogger(logger.Config{Output: io.Discard}))
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// place places orderID through h, the router of the api, and returns the response's status.
func place(h nethttp.Handler, orderID string) int {
	req := httptest.NewRequest("POST", "/orders", strings.NewReader(fmt.Sprintf(`{"order_id":%q}`, orderID)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// checkRead compares a message Outbox read with the one the endpoint saved. The databases normalize JSON and
// round times to microseconds.
func checkRead(t *testing.T, got, want worker.Message) {
	t.Helper()
	if got.ID != want.ID || got.Name != want.Name || got.Key != want.Key || got.Destination != want.Destination ||
		got.CreatedAt.Sub(want.CreatedAt).Abs() > time.Microsecond {
		t.Fatalf("read %+v, want %+v", got, want)
	}
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil {
		t.Fatalf("payload %s: %v", got.Payload, err)
	}
	_ = json.Unmarshal(want.Payload, &wantPayload)
	if !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatalf("payload %s, want %s", got.Payload, want.Payload)
	}
	if tenant, _ := got.Headers.Get("tenant"); tenant != "acme" {
		t.Fatalf("headers %v", got.Headers)
	}
}

func exec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func count(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

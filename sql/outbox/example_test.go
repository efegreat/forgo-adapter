package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/sql/outbox"
	"github.com/efegreat/forgo-adapter/sql/sqltx"
)

// Outbox saves messages in the transaction of ctx. The bootstrap matches it to the outbox.Postgres of
// the providers, so the service never names the adapter.
type Outbox interface {
	Save(ctx context.Context, msg worker.Message) error
}

type OrderPlaced struct {
	OrderID string `json:"orderId"`
}

type PlaceOrderReq struct {
	ID    string `json:"id" validate:"required"`
	Total int64  `json:"total" validate:"gt=0"`
}

type PlaceOrderResp struct {
	ID string `json:"id"`
}

type OrderService struct {
	db     *sql.DB
	tx     operation.Transactor
	outbox Outbox
}

func NewOrderService(db *sql.DB, tx operation.Transactor, outbox Outbox) *OrderService {
	return &OrderService{db: db, tx: tx, outbox: outbox}
}

// Place stores the order and its order.placed message in one transaction, so the producer publishes the
// message if and only if the order is committed.
func (s *OrderService) Place(ctx context.Context, req PlaceOrderReq) (PlaceOrderResp, error) {
	payload, err := json.Marshal(OrderPlaced{OrderID: req.ID})
	if err != nil {
		return PlaceOrderResp{}, err
	}
	msg := worker.NewMessage("order.placed", "order.placed", "orders", payload)

	err = s.tx.Do(ctx, func(ctx context.Context) error {
		if _, err := sqltx.From(ctx, s.db).ExecContext(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, req.ID, req.Total); err != nil {
			return err
		}
		return s.outbox.Save(ctx, msg)
	})
	if err != nil {
		return PlaceOrderResp{}, err
	}
	return PlaceOrderResp{ID: req.ID}, nil
}

// databaseConf is a sqldb.Config, and brokerConf the broker the producer publishes to, such as a
// rabbitmq.Config.
var databaseConf, brokerConf forgo.ResourceFactory

func ExamplePostgres_Save() {
	app := forgo.NewApp(
		forgo.WithName("orders"),
		forgo.WithResources(databaseConf, brokerConf),
		forgo.WithProviders(sqltx.NewTransactor, outbox.NewPostgres, NewOrderService),
		// The producer relays the saved messages to the broker. Serve OrderService.Place from an HTTP
		// server, in this app or in another one on the same database.
		forgo.WithServers(worker.ProducerConfig{}),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

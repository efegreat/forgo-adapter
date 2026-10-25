package inbox_test

import (
	"context"
	"database/sql"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/worker"

	"github.com/efegreat/forgo-adapter/sql/inbox"
	"github.com/efegreat/forgo-adapter/sql/sqltx"
)

type OrderPlaced struct {
	OrderID string `json:"orderId" validate:"required"`
}

type BillingService struct {
	db *sql.DB
}

func NewBillingService(db *sql.DB) *BillingService {
	return &BillingService{db: db}
}

// OnOrderPlaced writes in the transaction the consumer runs it in, which also marks the message completed
// in the inbox. A redelivery of a completed message is acknowledged without running it again.
func (s *BillingService) OnOrderPlaced(ctx context.Context, evt OrderPlaced) error {
	_, err := sqltx.From(ctx, s.db).ExecContext(ctx, `INSERT INTO invoices (order_id) VALUES ($1)`, evt.OrderID)
	return err
}

// databaseConf is a sqldb.Config, and brokerConf a broker with a "billing" subscriber, such as a
// rabbitmq.Config.
var databaseConf, brokerConf forgo.ResourceFactory

func ExampleNewPostgres() {
	consumer := worker.ConsumerConfig{
		Subscribers: []worker.SubscriberConfig{{
			Name: "billing",
			Handlers: []worker.HandlerConfig{
				{Name: "order.placed", Handler: worker.HandlerFor((*BillingService).OnOrderPlaced)},
			},
		}},
	}

	app := forgo.NewApp(
		forgo.WithName("billing-worker"),
		forgo.WithResources(databaseConf, brokerConf),
		forgo.WithProviders(sqltx.NewTransactor, inbox.NewPostgres, NewBillingService),
		forgo.WithServers(consumer),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

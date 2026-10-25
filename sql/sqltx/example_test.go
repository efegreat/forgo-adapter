package sqltx_test

import (
	"context"
	"database/sql"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/http"

	"github.com/efegreat/forgo-adapter/sql/sqldb"
	"github.com/efegreat/forgo-adapter/sql/sqltx"
)

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

// Insert runs on the transaction that ctx carries on r.db, and on r.db itself outside one.
func (r *OrderRepository) Insert(ctx context.Context, id string, total int64) error {
	_, err := sqltx.From(ctx, r.db).ExecContext(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, id, total)
	return err
}

func (r *OrderRepository) Audit(ctx context.Context, id, event string) error {
	_, err := sqltx.From(ctx, r.db).ExecContext(ctx, `INSERT INTO order_audit (order_id, event) VALUES ($1, $2)`, id, event)
	return err
}

type PlaceOrderReq struct {
	ID    string `json:"id" validate:"required"`
	Total int64  `json:"total" validate:"gt=0"`
}

type PlaceOrderResp struct {
	ID string `json:"id"`
}

type OrderService struct {
	tx     operation.Transactor
	orders *OrderRepository
}

// NewOrderService receives the sqltx.Transactor as an operation.Transactor.
func NewOrderService(tx operation.Transactor, orders *OrderRepository) *OrderService {
	return &OrderService{tx: tx, orders: orders}
}

// Place commits the order and its audit row together, or neither.
func (s *OrderService) Place(ctx context.Context, req PlaceOrderReq) (PlaceOrderResp, error) {
	err := s.tx.Do(ctx, func(ctx context.Context) error {
		if err := s.orders.Insert(ctx, req.ID, req.Total); err != nil {
			return err
		}
		return s.orders.Audit(ctx, req.ID, "placed")
	})
	if err != nil {
		return PlaceOrderResp{}, err
	}
	return PlaceOrderResp{ID: req.ID}, nil
}

func Example() {
	// main also imports the driver, such as _ "github.com/lib/pq".
	db := sqldb.Config{
		Name:       "main",
		DriverName: "postgres",
		DSN:        forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/shop?sslmode=disable"),
	}

	app := forgo.NewApp(
		forgo.WithName("orders-api"),
		forgo.WithResources(db),
		forgo.WithProviders(sqltx.NewTransactor, NewOrderRepository, NewOrderService),
		forgo.WithServers(http.Config{
			Addr: ":8080",
			Routes: []http.RouteConfig{
				{Name: "placeOrder", Method: "POST", Path: "/orders", Handler: http.HandlerFor((*OrderService).Place)},
			},
		}),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

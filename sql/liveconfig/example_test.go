package liveconfig_test

import (
	"context"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/server/http"

	"github.com/efegreat/forgo-adapter/sql/liveconfig"
)

// Checkout is a live config: an admin changes it while the application runs.
type Checkout struct {
	Enabled  bool `json:"enabled"`
	MaxItems int  `json:"max_items"`
}

func (Checkout) Name() string      { return "checkout" }
func (Checkout) Default() Checkout { return Checkout{Enabled: true, MaxItems: 50} }

var ErrCheckoutClosed = operation.Error{
	Code:     "checkout_closed",
	Status:   operation.StatusUnavailable,
	Message:  "checkout is closed",
	Severity: operation.SeverityLight,
}

type AddItemReq struct {
	CartID string `uri:"cart_id" validate:"required"`
	ItemID string `json:"item_id" validate:"required"`
}

type AddItemResp struct {
	Items int `json:"items"`
}

type CartService struct {
	configs *forgo.LiveConfigs
}

func NewCartService(configs *forgo.LiveConfigs) *CartService {
	return &CartService{configs: configs}
}

func (s *CartService) AddItem(ctx context.Context, req AddItemReq) (AddItemResp, error) {
	// Load reads the Redis cache, then the live_configs table, then Default.
	cfg, err := s.configs.Load[Checkout](ctx)
	if err != nil {
		return AddItemResp{}, err
	}
	if !cfg.Enabled {
		return AddItemResp{}, ErrCheckoutClosed
	}
	return AddItemResp{Items: 1}, nil
}

// databaseConf is a sqldb.Config, and redisConf a redisdb.Config of the redis module.
var databaseConf, redisConf forgo.ResourceFactory

// newCache is liveconfigcache.NewRedis of the redis module, github.com/efegreat/forgo-adapter/redis, which
// this module does not import.
var newCache any

func ExampleNewPostgres() {
	api := http.Config{
		Addr: ":8080",
		Routes: []http.RouteConfig{
			{Name: "addItem", Method: "POST", Path: "/carts/{cart_id}/items", Handler: http.HandlerFor((*CartService).AddItem)},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("cart-api"),
		forgo.WithResources(databaseConf, redisConf),
		forgo.WithProviders(
			liveconfig.NewPostgres, // forgo.LiveConfigStore: the live_configs table
			newCache,               // forgo.LiveConfigCache, which every instance shares
			forgo.NewLiveConfigs,
			NewCartService,
		),
		forgo.WithServers(api),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

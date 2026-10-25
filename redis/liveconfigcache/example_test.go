package liveconfigcache_test

import (
	"os"

	"github.com/efegreat/forgo"

	"github.com/efegreat/forgo-adapter/redis/liveconfigcache"
	"github.com/efegreat/forgo-adapter/redis/redisdb"
)

// databaseConf is a sqldb.Config of the root module, github.com/efegreat/forgo-adapter.
var databaseConf forgo.ResourceFactory

// newStore is liveconfig.NewPostgres of the root module, which this module does not import.
var newStore any

func ExampleNewRedis() {
	app := forgo.NewApp(
		forgo.WithName("cart-api"),
		forgo.WithResources(databaseConf, redisdb.Config{Addrs: []string{"localhost:6379"}}),
		forgo.WithProviders(
			newStore,                 // forgo.LiveConfigStore: the source of truth
			liveconfigcache.NewRedis, // forgo.LiveConfigCache, which every instance shares
			forgo.NewLiveConfigs,
		),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

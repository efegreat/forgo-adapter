package redisdb_test

import (
	"os"
	"strings"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/http"

	"github.com/efegreat/forgo-adapter/provider/locker"
	"github.com/efegreat/forgo-adapter/provider/ratelimit"
	"github.com/efegreat/forgo-adapter/resource/redisdb"
)

func ExampleConfig() {
	cache := redisdb.Config{
		Name:     "cache",
		Addrs:    strings.Split(forgo.Env("REDIS_ADDRS", "localhost:6379"), ","),
		Password: forgo.Env("REDIS_PASSWORD", ""),
	}

	api := http.Config{
		ServerConfig: forgo.ServerConfig{
			Middlewares: []forgo.MiddlewareFactory{
				// 100 requests per minute and client IP, counted in Redis across every instance.
				http.RateLimitWith(http.RateLimitConfig{Limit: 100, Period: time.Minute}),
			},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithResources(cache),
		// Both providers receive the client as a redis.UniversalClient.
		forgo.WithProviders(locker.NewRedis, ratelimit.NewRedis),
		forgo.WithServers(api),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

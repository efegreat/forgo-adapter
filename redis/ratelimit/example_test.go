package ratelimit_test

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/server/http"

	"github.com/efegreat/forgo-adapter/redis/ratelimit"
	"github.com/efegreat/forgo-adapter/redis/redisdb"
)

type LoginReq struct {
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required"`
}

type LoginResp struct {
	Token string `json:"token"`
}

type AuthService struct{}

func NewAuthService() *AuthService { return &AuthService{} }

func (s *AuthService) Login(ctx context.Context, req LoginReq) (LoginResp, error) {
	return LoginResp{}, nil
}

func ExampleNewRedis() {
	redis := redisdb.Config{
		Name:  "redis",
		Addrs: strings.Split(forgo.Env("REDIS_ADDRS", "localhost:6379"), ","),
	}
	api := http.Config{
		Addr: ":8080",
		Routes: []http.RouteConfig{{
			Name:    "login",
			Method:  "POST",
			Path:    "/login",
			Handler: http.HandlerFor((*AuthService).Login),
			Middlewares: []forgo.MiddlewareFactory{
				// 5 attempts per minute and client IP, counted in Redis across every instance. Over the
				// rate, the client gets 429 with a Retry-After header.
				http.RateLimitWith(http.RateLimitConfig{Limit: 5, Period: time.Minute, Prefix: "login:"}),
			},
		}},
	}

	app := forgo.NewApp(
		forgo.WithName("auth-api"),
		forgo.WithResources(redis),
		forgo.WithProviders(ratelimit.NewRedis, NewAuthService),
		forgo.WithServers(api),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

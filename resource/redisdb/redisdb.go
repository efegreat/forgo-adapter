package redisdb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/efegreat/forgo"

	"github.com/redis/go-redis/v9"
)

// Config is a Redis client resource. Its Value is a *redis.Client. Providers should request
// redis.UniversalClient, which it satisfies, so they also accept a cluster or failover client.
type Config struct {
	Name string
	// Addr is host:port. Default: localhost:6379.
	Addr     string
	Username string
	Password string
	// DB is the logical database selected after connecting.
	DB int
	// PoolSize is the maximum number of socket connections. Zero uses the go-redis default
	// (10 per GOMAXPROCS).
	PoolSize int
	// DialTimeout, ReadTimeout and WriteTimeout bound network operations. Zero uses the go-redis
	// defaults (5s dial, 3s read and write).
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// TLSConfig enables TLS when set.
	TLSConfig *tls.Config
	// ConnectTimeout bounds the ping New sends to verify Redis is reachable. Default: 5 seconds.
	ConnectTimeout time.Duration
}

func (c Config) Sanitize() Config {
	if c.Addr == "" {
		c.Addr = "localhost:6379"
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	return c
}

func (c Config) New() (forgo.Resource, error) {
	cfg := c.Sanitize()

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		TLSConfig:    cfg.TLSConfig,
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.Join(fmt.Errorf("redisdb: ping %s: %w", cfg.Addr, err), client.Close())
	}

	return &Redis{name: cfg.Name, client: client}, nil
}

type Redis struct {
	name   string
	client *redis.Client
}

func (r *Redis) Name() string {
	return r.name
}

func (r *Redis) Value() any {
	return r.client
}

func (r *Redis) Close() error {
	return r.client.Close()
}

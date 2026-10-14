// Package redisdb is a Redis resource: a go-redis universal client for one server, a Redis Cluster or a
// Sentinel-managed master, connected and verified when the application starts and closed when it stops.
package redisdb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/efegreat/forgo"

	"github.com/redis/go-redis/v9"
)

// ReadFrom chooses the nodes that serve the read-only commands of a cluster client or a failover
// client. Writes and scripts always go to the master. Replicas lag their master, so a read from
// a replica can miss a write that just succeeded.
type ReadFrom string

const (
	// ReadFromMaster sends every command to the master. The default.
	ReadFromMaster ReadFrom = "master"
	// ReadFromReplica sends read-only commands to a replica, and to the master while no replica
	// is healthy.
	ReadFromReplica ReadFrom = "replica"
	// ReadFromNearest sends read-only commands to the node with the lowest latency, master or
	// replica.
	ReadFromNearest ReadFrom = "nearest"
	// ReadFromAny sends read-only commands to a random node, master or replica.
	ReadFromAny ReadFrom = "any"
)

// Config is a go-redis universal client resource: one Redis server, a Redis Cluster or a
// Sentinel-managed master, chosen from Addrs, MasterName and ClusterMode the way
// redis.NewUniversalClient chooses. Its Value is a *redis.Client for a server or a master, and a
// *redis.ClusterClient for a cluster or for a master whose reads go to replicas (ReadFrom).
// Providers should request redis.UniversalClient, which both satisfy, so they work with every
// deployment shape.
type Config struct {
	// Name identifies the resource and must be unique in the bootstrap. `boot:"name"` tags select it
	// by this name when the bootstrap has several Redis deployments. Required.
	Name string
	// Addrs is a list of host:port addresses: the server, the seed nodes of a cluster, or the
	// Sentinels of MasterName. Two or more addresses without MasterName make a cluster client.
	// Default: localhost:6379.
	Addrs []string
	// MasterName is the master monitored by the Sentinels in Addrs. Setting it makes a failover
	// client, which follows the master the Sentinels promote.
	MasterName string
	// ClusterMode makes a cluster client from a single address, such as the configuration endpoint
	// of a managed cluster. It cannot be combined with MasterName.
	ClusterMode bool
	// ReadFrom chooses the nodes that serve read-only commands. A single-server client has one
	// node and ignores it, so one config can serve every environment. Default: ReadFromMaster.
	ReadFrom ReadFrom
	// Username and Password authenticate against the server, the cluster nodes or the master.
	Username string
	Password string
	// SentinelUsername and SentinelPassword authenticate against the Sentinels of MasterName, when
	// they differ from the master's credentials.
	SentinelUsername string
	SentinelPassword string
	// DB is the logical database selected after connecting. Redis Cluster only has database 0.
	DB int
	// PoolSize is the maximum number of socket connections, per node for a cluster. Zero uses the
	// go-redis default (10 per GOMAXPROCS).
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

// Sanitize returns a copy of the config with defaults applied to its zero fields.
func (c Config) Sanitize() Config {
	if len(c.Addrs) == 0 {
		c.Addrs = []string{"localhost:6379"}
	}
	if c.ReadFrom == "" {
		c.ReadFrom = ReadFromMaster
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	return c
}

// New validates the config, creates the client and pings Redis within ConnectTimeout, so that an
// unreachable deployment stops the application before it serves. Its Resource is a *Redis.
func (c Config) New() (forgo.Resource, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	client := cfg.client()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.Join(fmt.Errorf("redisdb: ping %s: %w", cfg.target(), err), client.Close())
	}

	return &Redis{name: cfg.Name, client: client}, nil
}

func (c Config) validate() error {
	for i, addr := range c.Addrs {
		if addr == "" {
			return fmt.Errorf("redisdb: Addrs[%d] is empty", i)
		}
	}
	if c.MasterName != "" && c.ClusterMode {
		return errors.New("redisdb: ClusterMode and MasterName cannot be combined")
	}
	switch c.ReadFrom {
	case ReadFromMaster, ReadFromReplica, ReadFromNearest, ReadFromAny:
	default:
		return fmt.Errorf("redisdb: unknown ReadFrom %q", c.ReadFrom)
	}
	if c.cluster() && c.DB != 0 {
		return fmt.Errorf("redisdb: DB %d is not supported by Redis Cluster, which only has database 0", c.DB)
	}
	return nil
}

// cluster reports whether redis.NewUniversalClient makes a cluster client from c.
func (c Config) cluster() bool {
	return c.MasterName == "" && (len(c.Addrs) > 1 || c.ClusterMode)
}

func (c Config) client() redis.UniversalClient {
	opts := &redis.UniversalOptions{
		// The cluster client appends the nodes it discovers to its seed list, which would write
		// into the backing array of a shared config.
		Addrs:            slices.Clone(c.Addrs),
		MasterName:       c.MasterName,
		IsClusterMode:    c.ClusterMode,
		ReadOnly:         c.ReadFrom == ReadFromReplica,
		RouteByLatency:   c.ReadFrom == ReadFromNearest,
		RouteRandomly:    c.ReadFrom == ReadFromAny,
		Username:         c.Username,
		Password:         c.Password,
		SentinelUsername: c.SentinelUsername,
		SentinelPassword: c.SentinelPassword,
		DB:               c.DB,
		PoolSize:         c.PoolSize,
		DialTimeout:      c.DialTimeout,
		ReadTimeout:      c.ReadTimeout,
		WriteTimeout:     c.WriteTimeout,
		TLSConfig:        c.TLSConfig,
	}
	if c.MasterName != "" && c.ReadFrom != ReadFromMaster {
		// A cluster client over the master and replicas the Sentinels report, which routes reads
		// like a cluster. NewUniversalClient would turn ReadOnly into a failover client that sends
		// every command, writes included, to a replica.
		return redis.NewFailoverClusterClient(opts.Failover())
	}
	return redis.NewUniversalClient(opts)
}

// target names what New pings, for its error.
func (c Config) target() string {
	addrs := strings.Join(c.Addrs, ",")
	if c.MasterName != "" {
		return c.MasterName + " via " + addrs
	}
	return addrs
}

// Redis is the resource New returns. Its Value is the redis.UniversalClient.
type Redis struct {
	name   string
	client redis.UniversalClient
}

// Name returns Config.Name.
func (r *Redis) Name() string {
	return r.name
}

// Value returns the client: a *redis.Client or a *redis.ClusterClient, as Config describes.
func (r *Redis) Value() any {
	return r.client
}

// Close closes the client and its connections.
func (r *Redis) Close() error {
	return r.client.Close()
}

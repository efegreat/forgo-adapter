package locker_test

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	"github.com/efegreat/forgo-adapter/provider/locker"
	"github.com/efegreat/forgo-adapter/resource/redisdb"
)

var ErrRebuildRunning = operation.Error{
	Code:     "rebuild_running",
	Status:   operation.StatusConflict,
	Message:  "a rebuild is already running",
	Severity: operation.SeverityLight,
}

type RebuildReq struct{}

type RebuildResp struct{}

type ReportService struct {
	locker operation.ILocker
}

// NewReportService receives the locker.Redis as an operation.ILocker. Cron jobs, commands and message
// handlers need no code for it: their Lock config makes the base chain's Locker take the lock.
func NewReportService(l operation.ILocker) *ReportService {
	return &ReportService{locker: l}
}

// Rebuild runs on one instance at a time, across every replica.
func (s *ReportService) Rebuild(ctx context.Context, req RebuildReq) (RebuildResp, error) {
	ok, err := s.locker.Lock(ctx, "report:rebuild", 10*time.Minute)
	if err != nil {
		return RebuildResp{}, err
	}
	if !ok {
		return RebuildResp{}, ErrRebuildRunning
	}
	// Unlock on a context that is not canceled, so the lock is released even when ctx ended.
	defer func() { _ = s.locker.Unlock(context.WithoutCancel(ctx), "report:rebuild") }()

	return RebuildResp{}, nil
}

// servers are the servers of the application, such as an http.Config with a route to Rebuild.
var servers []forgo.ServerFactory

func ExampleNewRedis() {
	redis := redisdb.Config{
		Name:  "redis",
		Addrs: strings.Split(forgo.Env("REDIS_ADDRS", "localhost:6379"), ","),
	}

	app := forgo.NewApp(
		forgo.WithName("reports-api"),
		forgo.WithResources(redis),
		forgo.WithProviders(locker.NewRedis, NewReportService),
		forgo.WithServers(servers...),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

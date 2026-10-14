package sqldb_test

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/efegreat/forgo"

	"github.com/efegreat/forgo-adapter/resource/sqldb"
)

type UserRepository struct {
	db *sql.DB
}

// NewUserRepository receives the *sql.DB of the resource.
func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Count(ctx context.Context) (n int, err error) {
	err = r.db.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&n)
	return n, err
}

// servers are the servers of the application, such as an http.Config.
var servers []forgo.ServerFactory

func ExampleConfig() {
	// main also imports the driver, such as _ "github.com/lib/pq".
	db := sqldb.Config{
		Name:         "main",
		DriverName:   "postgres",
		DSN:          forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/app?sslmode=disable"),
		MaxOpenConns: 20,
	}

	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithResources(db),
		forgo.WithProviders(NewUserRepository),
		forgo.WithServers(servers...),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

func ExampleLoggerConfig() {
	// Every statement logs through the logger of its context: at info, at warn from 500ms, and at
	// error when it fails. Arguments are logged, those of password_hash and token masked.
	db := sqldb.Config{
		Name:       "main",
		DriverName: "postgres",
		DSN:        forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/app?sslmode=disable"),
		Logger: sqldb.LoggerConfig{
			Enabled:         true,
			SlowThreshold:   500 * time.Millisecond,
			Args:            true,
			SensitiveFields: []string{"password_hash", "token"},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithResources(db),
		forgo.WithProviders(NewUserRepository),
		forgo.WithServers(servers...),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

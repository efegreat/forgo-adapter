package sqldb

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/efegreat/forgo/redact"
)

// planString renders a plan as one letter per ordinal: S when the argument is shown, M when it is
// masked.
func planString(p *paramPlan) string {
	var b strings.Builder
	for _, shown := range p.shown {
		if shown {
			b.WriteByte('S')
		} else {
			b.WriteByte('M')
		}
	}
	return b.String()
}

func TestParamPlan(t *testing.T) {
	fields := redact.NewFields("password", "token", "secret")
	for _, tc := range []struct {
		name, query, want string
	}{
		// Comparisons.
		{"equal", `SELECT * FROM users WHERE email = $1 AND password = $2`, "SM"},
		{"question marks", `SELECT * FROM users WHERE email = ? AND password = ?`, "SM"},
		{"qualified", `SELECT * FROM users u WHERE u.email <> $1 AND u.token != $2`, "SM"},
		{"reversed", `SELECT * FROM users WHERE $1 = email OR $2 = password`, "SM"},
		{"no spaces", `SELECT * FROM users WHERE email=$1 AND password=$2`, "SM"},
		{"like", `SELECT * FROM users WHERE email LIKE ? AND secret NOT ILIKE ?`, "SM"},
		{"similar to", `SELECT * FROM t WHERE name NOT SIMILAR TO $1`, "S"},
		{"is distinct from", `SELECT * FROM t WHERE name IS NOT DISTINCT FROM $1 AND token IS DISTINCT FROM $2`, "SM"},
		{"null safe", "SELECT * FROM t WHERE name <=> ? AND `token` <=> ?", "SM"},
		{"jsonb", `SELECT * FROM t WHERE tags @> $1 AND tags ? $2`, "SS"},
		{"case-insensitive name", `SELECT * FROM users WHERE "Password" = $1 AND PASSWORD = $2`, "MM"},
		{"quoted keyword", `SELECT * FROM t WHERE "select" = $1`, "S"},
		{"repeated", `SELECT * FROM users WHERE email = $1 OR token = $1`, "M"},
		{"repeated shown", `SELECT * FROM users WHERE email = $1 OR login = $1`, "S"},
		{"out of order", `SELECT * FROM users WHERE token = $2 AND email = $1`, "SM"},

		// Expressions around the placeholder.
		{"function", `SELECT * FROM users WHERE lower(email) = lower($1) AND lower(password) = lower($2)`, "SM"},
		{"cast", `SELECT * FROM users WHERE email = $1::citext AND $2::text = password`, "SM"},
		{"cast with brackets", `SELECT * FROM t WHERE id = ANY($1::varchar(20)[]) AND token = ANY($2::text[])`, "SM"},
		{"any", `SELECT * FROM t WHERE id = ANY($1) AND id <> ALL(ARRAY[$2, $3])`, "SSS"},
		{"call with column", `SELECT * FROM users WHERE password = crypt($1, password)`, "M"},
		{"concatenation", `SELECT * FROM t WHERE hash = sha256(salt || $1)`, "S"},
		{"concatenated sensitive", `SELECT * FROM t WHERE hash = sha256(secret || $1)`, "M"},
		{"arithmetic", `UPDATE t SET balance = balance + $1, total = $2 * 2 WHERE id = $3`, "SSS"},
		{"interval arithmetic", `SELECT * FROM t WHERE created_at > now() - $1::interval`, "S"},
		{"unary minus", `SELECT * FROM t WHERE delta = -$1`, "S"},
		{"json path", `SELECT * FROM t WHERE data->>'email' = $1 AND data->>'password' = $2`, "SM"},
		{"json path array", `SELECT * FROM t WHERE data #>> '{user,token}' = $1`, "M"},
		{"row", `SELECT * FROM t WHERE (a, b) = ($1, $2) AND (email, password) = ($3, $4)`, "SSMM"},
		{"named argument", `SELECT make_interval(secs => $1)`, "S"},

		// Lists, ranges and counts.
		{"in", `SELECT * FROM t WHERE id IN ($1, $2, $3)`, "SSS"},
		{"not in", `SELECT * FROM t WHERE id NOT IN (?, ?) AND token IN (?)`, "SSM"},
		{"between", `SELECT * FROM t WHERE created_at BETWEEN $1 AND $2 AND secret NOT BETWEEN $3 AND $4`, "SSMM"},
		{"between symmetric", `SELECT * FROM t WHERE n BETWEEN SYMMETRIC $1 AND $2`, "SS"},
		{"limit offset", `SELECT * FROM t ORDER BY id LIMIT $1 OFFSET $2`, "SS"},
		{"mysql limit", `SELECT * FROM t ORDER BY id LIMIT ?, ?`, "SS"},
		{"fetch first", `SELECT * FROM t ORDER BY id FETCH FIRST $1 ROWS ONLY`, "S"},
		{"mysql interval", `SELECT * FROM t WHERE processing_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND`, "S"},

		// Writes.
		{"set", `UPDATE users SET password = $1, name = $2 WHERE id = $3`, "MSS"},
		{"insert", `INSERT INTO users (email, password, name) VALUES ($1, $2, $3)`, "SMS"},
		{"insert rows", `INSERT INTO users (email, password) VALUES (?, ?), (?, ?), (?, ?)`, "SMSMSM"},
		{"insert expressions", `INSERT INTO users (email, token, created) VALUES (lower(?), ?, UTC_TIMESTAMP(6))`, "SM"},
		{"insert literals", `INSERT INTO live_configs (name, data, version, updated_at) VALUES (?, ?, 1, UTC_TIMESTAMP(6))`, "SS"},
		{"insert quoted columns", `INSERT INTO "users" ("email", "Token") VALUES ($1, $2)`, "SM"},
		{"insert without columns", `INSERT INTO users VALUES ($1, $2)`, "MM"},
		{"upsert", `INSERT INTO t (name, data) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET data = $2, token = $3`, "SSM"},
		{"insert select", `INSERT INTO t (a, b) SELECT $1, $2`, "MM"},

		// Placeholders nothing names.
		{"select list", `SELECT $1, COALESCE($2, name) FROM t`, "MM"},
		{"order by", `SELECT * FROM t ORDER BY $1`, "M"},
		{"case", `SELECT CASE WHEN status = $1 THEN $2 ELSE $3 END FROM t`, "SMM"},
		{"null test only", `SELECT * FROM t WHERE $1::text IS NULL`, "M"},
		{"null test and comparison", `SELECT * FROM t WHERE ($1::text IS NULL OR name = $1)`, "S"},
		{"null test and sensitive", `SELECT * FROM t WHERE ($1::text IS NULL OR token = $1)`, "M"},
		{"subquery", `SELECT * FROM t WHERE id IN (SELECT user_id FROM sessions WHERE token = $1 AND created > $2)`, "MS"},
		{"subquery select list", `SELECT * FROM t WHERE id IN (SELECT f($1) FROM s)`, "M"},
		{"exists", `SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id AND s.name = $1)`, "S"},

		// Text that only looks like placeholders.
		{"literal", `SELECT * FROM t WHERE note = 'password = ?' AND email = ?`, "S"},
		{"doubled quote", `SELECT * FROM t WHERE note = 'it''s ?' AND email = ?`, "S"},
		{"mysql backslash", `SELECT * FROM t WHERE note = 'it\'s ?' AND email = ?`, "S"},
		{"postgres backslash", `SELECT * FROM t WHERE path = 'C:\' AND email = $1`, "S"},
		{"escape string", `SELECT * FROM t WHERE note = E'it\'s $2' AND email = $1`, "S"},
		{"dollar quotes", `SELECT * FROM t WHERE body = $q$ token = $2 $q$ AND email = $1`, "S"},
		{"empty dollar quotes", `SELECT * FROM t WHERE body = $$ $2 $$ AND email = $1`, "S"},
		{"dollar in mysql literal", `SELECT * FROM t WHERE price = '$5' AND email = ?`, "S"},
		{"line comment", "SELECT * FROM t -- token = ?\nWHERE email = ?", "S"},
		{"hash comment", "SELECT * FROM t # token = ?\nWHERE email = ?", "S"},
		{"block comment", `SELECT * FROM t /* token = $2 /* nested */ */ WHERE email = $1`, "S"},
		{"identifier with dollar", `SELECT * FROM t WHERE a$1 = 1 AND email = $1`, "S"},
		{"postgres hash operator", `SELECT * FROM t WHERE flags # 1 = $1`, "S"},

		// Statements of forgo-adapter's storages.
		{"outbox claim", `UPDATE outbox_events SET processing_at = now() WHERE id = ANY(ARRAY(SELECT id FROM outbox_events WHERE processed_at IS NULL AND (processing_at IS NULL OR processing_at < now() - $1 * interval '1 microsecond') ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED)) RETURNING id, name, payload`, "SS"},
		{"inbox attempt", `INSERT INTO inbox_events (id, handler, status, attempt, updated_at) VALUES (?, ?, ?, 1, UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE attempt = attempt + 1`, "SSS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := planString(newParamPlan(tc.query, fields)); got != tc.want {
				t.Fatalf("plan of\n%s\n= %s, want %s", tc.query, got, tc.want)
			}
		})
	}
}

func TestParamPlanMasked(t *testing.T) {
	fields := redact.NewFields("token")
	plan := newParamPlan(`SELECT * FROM t WHERE email = $1 AND token = $2`, fields)
	for _, tc := range []struct {
		arg  driver.NamedValue
		want bool
	}{
		{driver.NamedValue{Ordinal: 1}, false},
		{driver.NamedValue{Ordinal: 2}, true},
		{driver.NamedValue{Ordinal: 3}, true}, // beyond the plan
		{driver.NamedValue{Ordinal: 0}, true},
		{driver.NamedValue{Ordinal: 1, Name: "TOKEN"}, true},
		{driver.NamedValue{Ordinal: 2, Name: "email"}, false},
	} {
		if got := plan.masked(tc.arg, fields); got != tc.want {
			t.Errorf("masked(%+v) = %v, want %v", tc.arg, got, tc.want)
		}
	}
}

func TestParamPlanLimits(t *testing.T) {
	fields := redact.NewFields("token")
	var b strings.Builder
	b.WriteString(`INSERT INTO t (a) VALUES `)
	for i := range maxArgs + 10 {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "($%d)", i+1)
	}
	plan := newParamPlan(b.String(), fields)
	if len(plan.shown) != maxArgs || planString(plan) != strings.Repeat("S", maxArgs) {
		t.Fatalf("plan of %d rows = %s", maxArgs+10, planString(plan))
	}

	huge := strings.Repeat("a + ", maxTokens) + `token = $1 AND email = $2`
	if got := planString(newParamPlan(huge, fields)); got != "" {
		t.Fatalf("placeholders past maxTokens = %s, want none", got)
	}
}

func TestPlanCache(t *testing.T) {
	fields := redact.NewFields("token")
	var c planCache
	query := `SELECT * FROM t WHERE token = $1`
	if first := c.get(query, fields); c.get(query, fields) != first {
		t.Fatal("plan not cached")
	}
	for i := range maxPlans + 5 {
		c.get(fmt.Sprintf(`SELECT * FROM t WHERE id IN (%s)`, strings.Repeat("$1, ", i)+"$1"), fields)
	}
	if got := c.size.Load(); got != maxPlans {
		t.Fatalf("cached %d plans, want %d", got, maxPlans)
	}
	if planString(c.get(`SELECT * FROM t WHERE email = $1`, fields)) != "S" {
		t.Fatal("plan not built once the cache is full")
	}
}

// FuzzParamPlan checks that any statement gets a plan, without panicking, that covers at most
// maxArgs arguments.
func FuzzParamPlan(f *testing.F) {
	for _, seed := range []string{
		`SELECT * FROM users WHERE email = $1 AND password = $2`,
		`INSERT INTO t (a, b) VALUES (?, ?), (lower(?), ?)`,
		`SELECT * FROM t WHERE id IN (SELECT f($1) FROM s) AND x BETWEEN $2 AND $3`,
		`SELECT $q$ $1 $q$, E'\'', '$1', /* $1 /* */ */ "a""b", a::b::c(1)[], ((($1)))`,
		"SELECT ? # ?\n -- ?\n LIMIT ?, ?",
		`)) ] ( [ $0 $999999999999999999999 :: , . => ?`,
	} {
		f.Add(seed)
	}
	fields := redact.NewFields("password", "token")
	f.Fuzz(func(t *testing.T, query string) {
		plan := newParamPlan(query, fields)
		if len(plan.shown) > maxArgs {
			t.Fatalf("plan covers %d arguments", len(plan.shown))
		}
	})
}

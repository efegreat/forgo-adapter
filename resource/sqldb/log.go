package sqldb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/redact"
	"github.com/rs/zerolog"

	"github.com/efegreat/forgo-adapter/internal/sqllog"
)

// LoggerConfig logs the statements run on the database, each on a line of its own written through
// logger.FromCtx, so the line carries the fields of the operation that ran it. Its zero value logs
// nothing, and the database then runs on its driver alone.
//
// A statement that completes within SlowThreshold logs at info, a slower one at warn, and a failed
// one at error with operation.SeverityCritical. A failure that is not the database's logs at warn
// with operation.SeverityLight: a statement canceled through its context, and driver.ErrBadConn,
// after which database/sql retries on another connection. The statements of forgo-adapter's
// storages log at debug instead of info.
type LoggerConfig struct {
	// Enabled turns statement logging on.
	Enabled bool
	// SlowThreshold is the duration from which a statement logs at warn as slow. A query lasts until
	// its rows are closed, so the time spent reading them counts. Default: 200 milliseconds.
	SlowThreshold time.Duration
	// SlowOnly logs only slow and failed statements.
	SlowOnly bool
	// Args logs the arguments of each statement under logger.SQLArgsLogKey, in placeholder order.
	Args bool
	// SensitiveFields masks logged arguments. An argument is logged as redact.Mask when the column
	// its placeholder is compared with, assigned to or inserted into is one of them, and the members
	// of a JSON argument that are are masked at any depth. Names match like the SensitiveFields of
	// forgo's servers: case-insensitively. While it is set, an argument whose column cannot be told
	// from the statement is masked too. Without it, every argument is logged as is.
	SensitiveFields []string
}

// Sanitize returns a copy of the config with defaults applied to its zero fields.
func (s LoggerConfig) Sanitize() LoggerConfig {
	if s.SlowThreshold == 0 {
		s.SlowThreshold = 200 * time.Millisecond
	}
	return s
}

func (s LoggerConfig) validate() error {
	if s.SlowThreshold < 0 {
		return errors.New("sqldb: Logger.SlowThreshold must not be negative")
	}
	return nil
}

// Messages of the lines the log writes.
const (
	completedMessage = "sql query completed"
	slowMessage      = "slow sql query"
	failedMessage    = "sql query failed"
)

// Limits of what one line holds.
const (
	// maxQueryLen is where a logged statement is cut, the limit of forgo's logged bodies.
	maxQueryLen = 32 << 10
	// maxArgLen is where a logged string or []byte argument is cut.
	maxArgLen = 1 << 10
	// maxArgs is how many arguments are logged.
	maxArgs = 100
	// maxCallerFrames bounds the frames searched for the caller of a statement.
	maxCallerFrames = 32
)

// callerSkipped are the packages a statement passes through on its way from the code that ran it.
var callerSkipped = []string{
	"database/sql.",
	reflect.TypeFor[queryLog]().PkgPath() + ".",
	"github.com/efegreat/forgo-adapter/provider/sqltx.",
	"runtime.",
}

var jsonNull = []byte("null")

// queryLog writes the lines of one database. It is built once, in New, and shared by every
// connection of the pool.
type queryLog struct {
	name     string
	slow     time.Duration
	slowOnly bool
	args     bool
	fields   redact.Fields
	plans    planCache
}

func newQueryLog(name string, cfg LoggerConfig) *queryLog {
	return &queryLog{
		name:     name,
		slow:     cfg.SlowThreshold,
		slowOnly: cfg.SlowOnly,
		args:     cfg.Args,
		fields:   redact.NewFields(cfg.SensitiveFields...),
	}
}

// statement is one statement the log reports, from the moment it was sent.
type statement struct {
	query string
	args  []driver.NamedValue
	start time.Time
	err   error
	// result is the result of an exec, for logger.SQLRowsAffectedLogKey.
	result driver.Result
	// rows counts the rows read from a query, for logger.SQLRowsReturnedLogKey, and is -1 for the
	// statements that return none.
	rows int64
}

// record logs s. Every argument is read before record returns, so the caller may reuse them.
func (l *queryLog) record(ctx context.Context, s statement) {
	elapsed := time.Since(s.start)
	var level zerolog.Level
	var severity operation.ErrorSeverity
	message := failedMessage
	switch {
	case s.err != nil && (ctx.Err() != nil || errors.Is(s.err, driver.ErrBadConn)):
		level, severity = zerolog.WarnLevel, operation.SeverityLight
	case s.err != nil:
		level, severity = zerolog.ErrorLevel, operation.SeverityCritical
	case elapsed >= l.slow:
		level, message = zerolog.WarnLevel, slowMessage
	case l.slowOnly:
		return
	case sqllog.IsQuiet(ctx):
		level, message = zerolog.DebugLevel, completedMessage
	default:
		level, message = zerolog.InfoLevel, completedMessage
	}
	event := logger.FromCtx(ctx).WithLevel(level)
	if !event.Enabled() {
		return
	}

	event.Str(logger.SQLDatabaseLogKey, l.name)
	if len(s.query) > maxQueryLen {
		event.Str(logger.SQLQueryLogKey, s.query[:runeCut(s.query, maxQueryLen)])
		event.Bool(logger.SQLQueryCutLogKey, true)
	} else {
		event.Str(logger.SQLQueryLogKey, s.query)
	}
	if l.args && len(s.args) > 0 {
		l.logArgs(event, s.query, s.args)
	}
	if s.result != nil {
		if n, err := s.result.RowsAffected(); err == nil {
			event.Int64(logger.SQLRowsAffectedLogKey, n)
		}
	}
	if s.rows >= 0 {
		event.Int64(logger.SQLRowsReturnedLogKey, s.rows)
	}
	event.Float64(logger.ElapsedTimeLogKey, elapsed.Seconds())
	if s.err != nil {
		event.Int(logger.ErrorSeverityLogKey, int(severity)).Err(s.err)
	}
	if level >= zerolog.WarnLevel {
		if c, ok := caller(); ok {
			event.Str(logger.SQLCallerLogKey, c)
		}
	}
	event.Msg(message)
}

// caller returns the file and line of the code that ran the statement being logged: the first frame
// outside database/sql, this package and sqltx. It only runs for slow and failed statements.
func caller() (string, bool) {
	var pcs [maxCallerFrames]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs[:])])
	for {
		frame, more := frames.Next()
		if frame.Function != "" && !skippedFrame(frame.Function) {
			return frame.File + ":" + strconv.Itoa(frame.Line), true
		}
		if !more {
			return "", false
		}
	}
}

func skippedFrame(function string) bool {
	for _, prefix := range callerSkipped {
		if strings.HasPrefix(function, prefix) {
			return true
		}
	}
	return false
}

// logArgs adds the arguments to event, masked by the plan of the statement.
func (l *queryLog) logArgs(event *zerolog.Event, query string, args []driver.NamedValue) {
	var plan *paramPlan
	if !l.fields.Empty() {
		plan = l.plans.get(query, l.fields)
	}
	arr := zerolog.Arr()
	cut := len(args) > maxArgs
	for _, arg := range args[:min(len(args), maxArgs)] {
		if plan != nil && plan.masked(arg, l.fields) {
			arr.Str(redact.Mask)
			continue
		}
		if l.appendArg(arr, arg.Value) {
			cut = true
		}
	}
	event.Array(logger.SQLArgsLogKey, arr)
	if cut {
		event.Bool(logger.SQLArgsCutLogKey, true)
	}
}

// appendArg appends v, a value the driver accepted, and reports whether it was cut.
func (l *queryLog) appendArg(arr *zerolog.Array, v driver.Value) (cut bool) {
	switch v := v.(type) {
	case nil:
		arr.RawJSON(jsonNull)
	case int64:
		arr.Int64(v)
	case float64:
		arr.Float64(v)
	case bool:
		arr.Bool(v)
	case time.Time:
		arr.Time(v)
	case string:
		return appendText(arr, l.fields, v, (*zerolog.Array).Str)
	case []byte:
		return appendText(arr, l.fields, v, (*zerolog.Array).Bytes)
	default:
		// A type the driver takes as is, through its NamedValueChecker. It is logged the way
		// encoding/json encodes it, so its sensitive members are masked like those of a JSON string.
		data, err := json.Marshal(v)
		if err != nil {
			arr.Str(fmt.Sprintf("%T", v))
			return false
		}
		if cut = len(data) > maxArgLen; cut {
			data = data[:runeCut(data, maxArgLen)]
		}
		switch {
		case !l.fields.Empty():
			return appendJSON(arr, l.fields, data) || cut
		case cut:
			arr.Bytes(data)
		default:
			arr.RawJSON(data)
		}
		return cut
	}
	return false
}

// appendText appends a string or []byte argument through add, cut at maxArgLen. One that holds
// JSON is masked while there are sensitive fields.
func appendText[S ~string | ~[]byte](arr *zerolog.Array, fields redact.Fields, v S, add func(*zerolog.Array, S) *zerolog.Array) (cut bool) {
	if cut = len(v) > maxArgLen; cut {
		v = v[:runeCut(v, maxArgLen)]
	}
	if fields.Empty() || !isJSON(v) {
		add(arr, v)
		return cut
	}
	return appendJSON(arr, fields, v) || cut
}

// appendJSON appends a JSON value with its sensitive members masked. A whole value is logged as
// JSON. One that is cut or invalid is logged as a string, up to where it stops being valid, and
// reported as cut, since the rest cannot be masked.
func appendJSON[S ~string | ~[]byte](arr *zerolog.Array, fields redact.Fields, v S) (cut bool) {
	bufp := jsonBuffers.Get().(*[]byte)
	defer jsonBuffers.Put(bufp)
	masked, valid := fields.AppendJSON((*bufp)[:0], v)
	*bufp = masked[:0]
	if valid {
		arr.RawJSON(masked)
		return false
	}
	arr.Bytes(masked)
	return true
}

// jsonBuffers hold masked JSON arguments until they are copied into their line. Arguments are cut
// at maxArgLen, so no buffer grows much past it.
var jsonBuffers = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 2*maxArgLen)
		return &buf
	},
}

// isJSON reports whether v starts like a JSON object or array.
func isJSON[S ~string | ~[]byte](v S) bool {
	for i := range len(v) {
		switch v[i] {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		}
		return false
	}
	return false
}

// runeCut returns the length of the longest prefix of s within limit bytes that does not split a
// rune.
func runeCut[S ~string | ~[]byte](s S, limit int) int {
	n := limit
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return n
}

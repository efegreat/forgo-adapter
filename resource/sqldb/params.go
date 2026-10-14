package sqldb

import (
	"database/sql/driver"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/efegreat/forgo/redact"
)

// A paramPlan tells, for each placeholder of a statement, whether its argument may be logged while
// there are sensitive fields. Arguments reach the driver by position only, so the name an argument
// is masked by is read from the statement: the column its placeholder is compared with, assigned to
// or inserted into. The statement is read once, when a line first logs its arguments, and the plan
// is cached under its text, so a line costs one map lookup.
//
// Reading the statement is a heuristic. It follows the common forms, listed in params_test.go, and
// masks an argument whenever it is unsure: a placeholder it cannot name, one named after a sensitive
// field anywhere around it (both columns of salt || $1 = hash), and a $n that appears several times
// unless every appearance can be shown.
type paramPlan struct {
	shown []bool // by ordinal - 1
}

// masked reports whether arg must be logged as redact.Mask. A named argument, as sql.Named passes,
// is masked by its own name.
func (p *paramPlan) masked(arg driver.NamedValue, fields redact.Fields) bool {
	if arg.Name != "" {
		return fields.Has(arg.Name)
	}
	i := arg.Ordinal - 1
	return i < 0 || i >= len(p.shown) || !p.shown[i]
}

// maxPlans bounds the plans cached for one database, against statements built with a varying number
// of placeholders. Past it, plans are built for each line without being cached.
const maxPlans = 1024

type planCache struct {
	plans sync.Map // query string → *paramPlan
	size  atomic.Int32
}

func (c *planCache) get(query string, fields redact.Fields) *paramPlan {
	if p, ok := c.plans.Load(query); ok {
		return p.(*paramPlan)
	}
	p := newParamPlan(query, fields)
	if c.size.Load() < maxPlans {
		if _, loaded := c.plans.LoadOrStore(query, p); !loaded {
			c.size.Add(1)
		}
	}
	return p
}

// verdict is what one appearance of a placeholder says about its argument.
type verdict uint8

const (
	masked  verdict = iota
	shown           // named, and no name around it is sensitive
	neutral         // a test such as $1 IS NULL, which says nothing either way
)

func newParamPlan(query string, fields redact.Fields) *paramPlan {
	toks := tokenize(query)
	r := resolver{toks: toks, fields: fields}
	var verdicts []verdict // by ordinal - 1, neutral until seen
	for i, t := range toks {
		if t.kind != tokParam || t.param < 1 || t.param > maxArgs {
			continue
		}
		for len(verdicts) < t.param {
			verdicts = append(verdicts, neutral)
		}
		switch v := r.verdict(i); {
		case v == masked:
			verdicts[t.param-1] = masked
		case v == shown && verdicts[t.param-1] == neutral:
			verdicts[t.param-1] = shown
		}
	}
	plan := &paramPlan{shown: make([]bool, len(verdicts))}
	for i, v := range verdicts {
		plan.shown[i] = v == shown
	}
	return plan
}

type tokenKind uint8

const (
	tokIdent   tokenKind = iota // a word or a quoted identifier
	tokParam                    // a placeholder
	tokLiteral                  // a string or a number
	tokOp                       // an operator
	tokOpen                     // ( or [
	tokClose                    // ) or ]
	tokComma
	tokDot
	tokOther
)

type token struct {
	kind tokenKind
	// text is a word, an identifier without its quotes, an operator, or a literal as written.
	text   string
	quoted bool // a quoted identifier, never a keyword
	param  int  // the ordinal of a placeholder
	match  int  // the index of the matching bracket, or -1
	group  int  // the index of the innermost bracket around the token, or -1
}

// maxTokens bounds the tokens read from one statement. Placeholders past it are masked.
const maxTokens = 4096

// tokenize splits a statement into tokens, skipping comments. Placeholders are $n, as PostgreSQL
// numbers them, or ?, as MySQL does, in which case literals take backslash escapes and # starts a
// comment. A statement with a $ is read with PostgreSQL's rules first, and again with MySQL's when
// that finds no $n outside literals and comments.
func tokenize(query string) []token {
	if strings.IndexByte(query, '$') >= 0 {
		if toks, sawDollar := tokenizeWith(query, true); sawDollar {
			return toks
		}
	}
	toks, _ := tokenizeWith(query, false)
	return toks
}

func tokenizeWith(q string, dollar bool) (toks []token, sawDollar bool) {
	var open []int // indexes of the brackets not closed yet
	questions := 0
	add := func(t token) {
		t.match, t.group = -1, -1
		if len(open) > 0 {
			t.group = open[len(open)-1]
		}
		toks = append(toks, t)
	}
	for i := 0; i < len(q) && len(toks) < maxTokens; {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && strings.HasPrefix(q[i:], "--"), c == '#' && !dollar:
			i = lineEnd(q, i)
		case c == '/' && strings.HasPrefix(q[i:], "/*"):
			i = commentEnd(q, i)
		case c == '\'':
			end := stringEnd(q, i, !dollar)
			add(token{kind: tokLiteral, text: q[i:end]})
			i = end
		case c == '"' || c == '`':
			end := quotedEnd(q, i, c)
			add(token{kind: tokIdent, text: q[i+1 : max(i+1, end-1)], quoted: true})
			i = end
		case c == '$' && dollar && i+1 < len(q) && isDigit(q[i+1]):
			j := i + 1
			n := 0
			for j < len(q) && isDigit(q[j]) {
				n = min(n*10+int(q[j]-'0'), maxArgs+1)
				j++
			}
			sawDollar = true
			add(token{kind: tokParam, param: n})
			i = j
		case c == '$' && dollar:
			end, ok := dollarQuoteEnd(q, i)
			if !ok {
				add(token{kind: tokOp, text: "$"})
				i++
				break
			}
			add(token{kind: tokLiteral, text: q[i:end]})
			i = end
		case c == '?' && !dollar:
			questions++
			add(token{kind: tokParam, param: questions})
			i++
		case isDigit(c) || c == '.' && i+1 < len(q) && isDigit(q[i+1]):
			j := i + 1
			for j < len(q) && (isWordByte(q[j]) || q[j] == '.') {
				j++
			}
			add(token{kind: tokLiteral, text: q[i:j]})
			i = j
		case isWordStart(c):
			j := i + 1
			for j < len(q) && isWordByte(q[j]) {
				j++
			}
			if j < len(q) && q[j] == '\'' {
				// A literal with a prefix: E'…', N'…', X'…', _utf8mb4'…'.
				end := stringEnd(q, j, !dollar || j == i+1 && (c == 'E' || c == 'e'))
				add(token{kind: tokLiteral, text: q[i:end]})
				i = end
				break
			}
			add(token{kind: tokIdent, text: q[i:j]})
			i = j
		case c == '(' || c == '[':
			add(token{kind: tokOpen, text: q[i : i+1]})
			open = append(open, len(toks)-1)
			i++
		case c == ')' || c == ']':
			if len(open) > 0 {
				o := open[len(open)-1]
				open = open[:len(open)-1]
				add(token{kind: tokClose, text: q[i : i+1]})
				toks[o].match, toks[len(toks)-1].match = len(toks)-1, o
			} else {
				add(token{kind: tokClose, text: q[i : i+1]})
			}
			i++
		case c == ',':
			add(token{kind: tokComma})
			i++
		case c == '.':
			add(token{kind: tokDot})
			i++
		case isOpByte(c, dollar):
			j := i + 1
			for j < len(q) && isOpByte(q[j], dollar) && !strings.HasPrefix(q[j:], "--") && !strings.HasPrefix(q[j:], "/*") {
				j++
			}
			add(token{kind: tokOp, text: q[i:j]})
			i = j
		default:
			add(token{kind: tokOther})
			i++
		}
	}
	return toks, sawDollar
}

func lineEnd(q string, i int) int {
	if j := strings.IndexByte(q[i:], '\n'); j >= 0 {
		return i + j + 1
	}
	return len(q)
}

// commentEnd returns the end of the block comment at i. Comments nest, as in PostgreSQL. MySQL ends
// them at the first */, so a nested comment there hides what follows, whose arguments are masked.
func commentEnd(q string, i int) int {
	depth := 0
	for i < len(q) {
		switch {
		case strings.HasPrefix(q[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(q[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(q)
}

// stringEnd returns the end of the string literal whose opening quote is at i. A doubled quote is a
// quote, and so is an escaped one when backslashes escape.
func stringEnd(q string, i int, backslash bool) int {
	for j := i + 1; j < len(q); j++ {
		switch q[j] {
		case '\\':
			if backslash {
				j++
			}
		case '\'':
			if j+1 < len(q) && q[j+1] == '\'' {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(q)
}

// quotedEnd returns the end of the identifier quoted with c at i. A doubled quote is a quote.
func quotedEnd(q string, i int, c byte) int {
	for j := i + 1; j < len(q); j++ {
		if q[j] == c {
			if j+1 < len(q) && q[j+1] == c {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(q)
}

// dollarQuoteEnd returns the end of the PostgreSQL dollar-quoted string at i, $tag$…$tag$.
func dollarQuoteEnd(q string, i int) (int, bool) {
	j := i + 1
	for j < len(q) && (isWordByte(q[j]) && q[j] != '$') {
		j++
	}
	if j == len(q) || q[j] != '$' || j > i+1 && isDigit(q[i+1]) {
		return 0, false
	}
	tag := q[i : j+1]
	if k := strings.Index(q[j+1:], tag); k >= 0 {
		return j + 1 + k + len(tag), true
	}
	return len(q), true
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func isWordStart(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_' || c >= 0x80
}

func isWordByte(c byte) bool {
	return isWordStart(c) || isDigit(c) || c == '$'
}

func isOpByte(c byte, dollar bool) bool {
	switch c {
	case '+', '-', '*', '/', '<', '>', '=', '~', '!', '@', '%', '^', '&', '|', ':':
		return true
	case '#', '?':
		return dollar
	}
	return false
}

// resolver decides what each appearance of a placeholder says about its argument.
type resolver struct {
	toks   []token
	fields redact.Fields
	// sensitive is set when a name met while resolving the current placeholder is sensitive.
	sensitive bool
}

// maxSteps bounds how far a placeholder is followed out of the expressions around it.
const maxSteps = 64

// verdict follows the placeholder at p outwards, through the operators and brackets around it,
// until it finds what names it: a comparison, IN, BETWEEN, the column list of an INSERT, or LIMIT and
// its kin. The span [lo, hi] is the expression that holds the placeholder so far.
func (r *resolver) verdict(p int) verdict {
	r.sensitive = false
	lo, hi := p, r.castsAfter(p)
	for range maxSteps {
		prev, next := lo-1, hi+1
		if r.nullTest(next) {
			return neutral
		}
		if start, ok := r.comparisonBefore(prev); ok {
			return r.operand(r.chainBefore(start-1), start-1)
		}
		if end, ok := r.comparisonAfter(next); ok {
			return r.operand(end+1, r.chainAfter(end+1))
		}
		switch {
		case r.keyword(prev, "BETWEEN"), r.keyword(prev, "SYMMETRIC") && r.keyword(prev-1, "BETWEEN"):
			return r.between(prev)
		case r.keyword(prev, "AND"):
			if s := r.chainBefore(prev - 1); s > 0 && r.keyword(s-1, "BETWEEN") {
				return r.between(s - 1)
			} else if s > 1 && r.keyword(s-1, "SYMMETRIC") && r.keyword(s-2, "BETWEEN") {
				return r.between(s - 1)
			}
		case r.countKeyword(prev):
			return r.decide(true)
		case r.is(prev, tokComma) && r.is(prev-1, tokParam) && r.keyword(prev-2, "LIMIT"):
			return r.decide(true) // LIMIT ?, ?
		case r.op(prev, "=>") || r.op(prev, ":="):
			// A named argument, make_interval(secs => $1), or an assignment.
			if r.is(prev-1, tokIdent) {
				return r.decide(r.name(r.toks[prev-1].text))
			}
			return masked
		}
		if r.arithmetic(prev) {
			if s := r.chainBefore(prev - 1); s >= 0 {
				r.names(s, prev-1)
				lo = s
			} else {
				lo = prev // a unary operator
			}
			continue
		}
		if r.arithmetic(next) {
			e := r.chainAfter(next + 1)
			if e < 0 {
				return masked
			}
			r.names(next+1, e)
			hi = e
			continue
		}

		// Nothing names the span where it is: leave the brackets around it.
		g := r.toks[lo].group
		if g < 0 || r.toks[g].match < 0 || r.subquery(g) {
			return masked
		}
		owner := g - 1
		if r.keyword(owner, "IN") {
			return r.operandBefore(r.skipNot(owner - 1))
		}
		if values, ok := r.valuesOf(g); ok {
			column, ok := r.insertColumn(values, r.element(g, lo))
			return r.decide(ok && r.name(column))
		}
		if r.is(owner, tokIdent) {
			lo = r.qualifiedStart(owner) // a call: lower($1), ANY($1), CAST($1 AS …)
		} else {
			lo = g
		}
		hi = r.castsAfter(r.toks[g].match)
	}
	return masked
}

// decide returns the verdict of a placeholder that was named, or not.
func (r *resolver) decide(named bool) verdict {
	if !named || r.sensitive {
		return masked
	}
	return shown
}

// operand returns the verdict of a placeholder compared with the operand [s, e].
func (r *resolver) operand(s, e int) verdict {
	if s < 0 || e < s {
		return masked
	}
	return r.decide(r.names(s, e))
}

// operandBefore returns the verdict of a placeholder compared with the operand ending at e.
func (r *resolver) operandBefore(e int) verdict {
	return r.operand(r.chainBefore(e), e)
}

// between returns the verdict of a bound of the BETWEEN at b, or of BETWEEN SYMMETRIC at b-1.
func (r *resolver) between(b int) verdict {
	if r.keyword(b, "SYMMETRIC") {
		b--
	}
	return r.operandBefore(r.skipNot(b - 1))
}

// names reports whether the tokens [s, e] name columns, and notes when one is sensitive. It takes
// every identifier that is not a keyword, a qualifier, a function or a type, and the keys of JSON
// paths, data->>'email'. Taking too many names can only mask more.
func (r *resolver) names(s, e int) bool {
	found := false
	for i := s; i <= e; i++ {
		t := r.toks[i]
		switch t.kind {
		case tokIdent:
			if !t.quoted && isKeyword(t.text) || r.is(i+1, tokDot) || r.is(i+1, tokOpen) || r.op(i-1, "::") || r.keyword(i-1, "AS") {
				continue
			}
			found = r.name(t.text) || found
		case tokLiteral:
			if r.jsonPath(i - 1) {
				found = jsonPathNames(t.text, r.name) || found
			}
		}
	}
	return found
}

// name notes a name, and reports that there was one.
func (r *resolver) name(name string) bool {
	if r.fields.Has(name) {
		r.sensitive = true
	}
	return true
}

// jsonPathNames calls fn with the keys of a JSON path literal, 'email' or '{user,email}'.
func jsonPathNames(literal string, fn func(string) bool) bool {
	literal = strings.Trim(literal, "'")
	literal = strings.TrimSuffix(strings.TrimPrefix(literal, "{"), "}")
	found := false
	for key := range strings.SplitSeq(literal, ",") {
		if key = strings.Trim(strings.TrimSpace(key), `"`); key != "" {
			found = fn(key) || found
		}
	}
	return found
}

// chainBefore returns the start of the operand ending at e, with the operands joined to it by
// operators that bind tighter than comparisons: a + b, data->>'k'. It returns -1 when e ends none.
func (r *resolver) chainBefore(e int) int {
	s := r.operandStart(e)
	for s >= 2 && r.arithmetic(s-1) {
		prev := r.operandStart(s - 2)
		if prev < 0 {
			break
		}
		s = prev
	}
	return s
}

// chainAfter is chainBefore forwards: the end of the operands starting at s, or -1.
func (r *resolver) chainAfter(s int) int {
	e := r.operandEnd(s)
	for e >= 0 && r.arithmetic(e+1) {
		next := r.operandEnd(e + 2)
		if next < 0 {
			break
		}
		e = next
	}
	return e
}

// operandStart returns the start of the single operand ending at e: a column, a call, a bracket, a
// literal or a placeholder, with any cast. It returns -1 when e ends none.
func (r *resolver) operandStart(e int) int {
	for steps := 0; e >= 0 && e < len(r.toks) && steps < maxSteps; steps++ {
		var s int
		switch t := r.toks[e]; t.kind {
		case tokIdent:
			s = r.qualifiedStart(e)
		case tokClose:
			if t.match < 0 {
				return -1
			}
			s = t.match
			if r.is(s-1, tokIdent) && !r.keyword(s-1, "IN") {
				s = r.qualifiedStart(s - 1)
			}
		case tokLiteral, tokParam:
			s = e
		default:
			return -1
		}
		if !r.op(s-1, "::") {
			return s
		}
		e = s - 2 // s starts the type of a cast: the operand is what is cast
	}
	return -1
}

// operandEnd returns the end of the single operand starting at s, or -1.
func (r *resolver) operandEnd(s int) int {
	for s < len(r.toks) && (r.op(s, "-") || r.op(s, "+") || r.op(s, "~") || r.keyword(s, "NOT")) {
		s++
	}
	if s < 0 || s >= len(r.toks) {
		return -1
	}
	var e int
	switch t := r.toks[s]; t.kind {
	case tokIdent:
		e = r.qualifiedEnd(s)
		if r.is(e+1, tokOpen) && r.toks[e+1].match >= 0 {
			e = r.toks[e+1].match
		}
	case tokOpen:
		if t.match < 0 {
			return -1
		}
		e = t.match
	case tokLiteral, tokParam:
		e = s
	default:
		return -1
	}
	return r.castsAfter(e)
}

// castsAfter returns the end of the casts that follow e, $1::uuid or $1::varchar(10)[], or e.
func (r *resolver) castsAfter(e int) int {
	for r.op(e+1, "::") && r.is(e+2, tokIdent) {
		e = r.qualifiedEnd(e + 2)
		for r.is(e+1, tokOpen) && r.toks[e+1].match >= 0 {
			e = r.toks[e+1].match
		}
	}
	return e
}

func (r *resolver) qualifiedStart(i int) int {
	for r.is(i-1, tokDot) && r.is(i-2, tokIdent) {
		i -= 2
	}
	return i
}

func (r *resolver) qualifiedEnd(i int) int {
	for r.is(i+1, tokDot) && r.is(i+2, tokIdent) {
		i += 2
	}
	return i
}

// comparisonBefore reports whether a comparison ends at i, and where it starts: =, LIKE, NOT LIKE,
// SIMILAR TO, IS DISTINCT FROM and their kin.
func (r *resolver) comparisonBefore(i int) (start int, ok bool) {
	switch {
	case r.is(i, tokOp) && comparisonOps[r.toks[i].text]:
		return i, true
	case r.likeKeyword(i):
		return r.skipNot(i-1) + 1, true
	case r.keyword(i, "TO") && r.keyword(i-1, "SIMILAR"):
		return r.skipNot(i-2) + 1, true
	case r.keyword(i, "FROM") && r.keyword(i-1, "DISTINCT"):
		if r.keyword(i-2, "IS") {
			return i - 2, true
		}
		if r.keyword(i-2, "NOT") && r.keyword(i-3, "IS") {
			return i - 3, true
		}
	}
	return 0, false
}

// comparisonAfter is comparisonBefore forwards: whether a comparison starts at i, and its end.
func (r *resolver) comparisonAfter(i int) (end int, ok bool) {
	if r.keyword(i, "NOT") {
		i++
	}
	switch {
	case r.is(i, tokOp) && comparisonOps[r.toks[i].text] && !r.keyword(i-1, "NOT"):
		return i, true
	case r.likeKeyword(i):
		return i, true
	case r.keyword(i, "SIMILAR") && r.keyword(i+1, "TO"):
		return i + 1, true
	case r.keyword(i, "IS"):
		j := i + 1
		if r.keyword(j, "NOT") {
			j++
		}
		if r.keyword(j, "DISTINCT") && r.keyword(j+1, "FROM") {
			return j + 1, true
		}
	}
	return 0, false
}

func (r *resolver) likeKeyword(i int) bool {
	return r.keyword(i, "LIKE") || r.keyword(i, "ILIKE") || r.keyword(i, "REGEXP") || r.keyword(i, "RLIKE") || r.keyword(i, "GLOB")
}

// nullTest reports whether IS [NOT] NULL, TRUE, FALSE or UNKNOWN starts at i.
func (r *resolver) nullTest(i int) bool {
	if !r.keyword(i, "IS") {
		return false
	}
	if r.keyword(i+1, "NOT") {
		i++
	}
	return r.keyword(i+1, "NULL") || r.keyword(i+1, "TRUE") || r.keyword(i+1, "FALSE") || r.keyword(i+1, "UNKNOWN")
}

// countKeyword reports whether i is a keyword whose value is a count or a length of time.
func (r *resolver) countKeyword(i int) bool {
	return r.keyword(i, "LIMIT") || r.keyword(i, "OFFSET") || r.keyword(i, "TOP") || r.keyword(i, "INTERVAL") ||
		(r.keyword(i, "FIRST") || r.keyword(i, "NEXT")) && r.keyword(i-1, "FETCH")
}

// arithmetic reports whether i is a binary operator that binds tighter than comparisons.
func (r *resolver) arithmetic(i int) bool {
	return r.is(i, tokOp) && arithmeticOps[r.toks[i].text] || r.keyword(i, "DIV") || r.keyword(i, "MOD")
}

// jsonPath reports whether i is a JSON path operator, whose right operand is a key or path.
func (r *resolver) jsonPath(i int) bool {
	if !r.is(i, tokOp) {
		return false
	}
	switch r.toks[i].text {
	case "->", "->>", "#>", "#>>", "#-":
		return true
	}
	return false
}

// subquery reports whether the bracket at g opens a subquery, whose placeholders are named inside it
// or not at all.
func (r *resolver) subquery(g int) bool {
	return r.keyword(g+1, "SELECT") || r.keyword(g+1, "WITH") || r.keyword(g+1, "VALUES") || r.keyword(g+1, "TABLE")
}

// valuesOf returns the VALUES keyword whose rows include the bracket at g.
func (r *resolver) valuesOf(g int) (int, bool) {
	for t := g; ; {
		p := t - 1
		if r.keyword(p, "VALUES") || r.keyword(p, "VALUE") {
			return p, true
		}
		if !r.is(p, tokComma) || !r.is(p-1, tokClose) || r.toks[p-1].match < 0 {
			return 0, false
		}
		t = r.toks[p-1].match
	}
}

// element returns the position, among the elements of the bracket at g, of the one at i.
func (r *resolver) element(g, i int) int {
	n := 0
	for j := g + 1; j < i; j++ {
		if r.toks[j].kind == tokComma && r.toks[j].group == g {
			n++
		}
	}
	return n
}

// insertColumn returns the column at position n in the column list before the VALUES at v.
func (r *resolver) insertColumn(v, n int) (string, bool) {
	end := v - 1
	if !r.is(end, tokClose) || r.toks[end].match < 0 {
		return "", false
	}
	open := r.toks[end].match
	column, k := "", 0
	for j := open + 1; j <= end; j++ {
		t := r.toks[j]
		if t.group != open && j != end {
			continue
		}
		switch {
		case j == end || t.kind == tokComma:
			if k == n {
				return column, column != ""
			}
			k++
			column = ""
		case t.kind == tokIdent && !r.is(j+1, tokDot):
			column = t.text
		}
	}
	return "", false
}

func (r *resolver) skipNot(i int) int {
	if r.keyword(i, "NOT") {
		return i - 1
	}
	return i
}

func (r *resolver) is(i int, kind tokenKind) bool {
	return i >= 0 && i < len(r.toks) && r.toks[i].kind == kind
}

func (r *resolver) op(i int, op string) bool {
	return r.is(i, tokOp) && r.toks[i].text == op
}

func (r *resolver) keyword(i int, keyword string) bool {
	return r.is(i, tokIdent) && !r.toks[i].quoted && strings.EqualFold(r.toks[i].text, keyword)
}

var comparisonOps = map[string]bool{
	"=": true, "==": true, "<>": true, "!=": true, "<": true, ">": true, "<=": true, ">=": true, "<=>": true,
	"~": true, "~*": true, "!~": true, "!~*": true, "~~": true, "~~*": true, "!~~": true, "!~~*": true,
	"@>": true, "<@": true, "&&": true, "?": true, "?|": true, "?&": true, "@@": true, "@?": true, "^@": true,
}

var arithmeticOps = map[string]bool{
	"+": true, "-": true, "*": true, "/": true, "%": true, "||": true, "|": true, "&": true, "^": true,
	"<<": true, ">>": true, "#": true, "->": true, "->>": true, "#>": true, "#>>": true, "#-": true,
}

// isKeyword reports whether a word is a keyword that never names a column.
func isKeyword(word string) bool {
	if len(word) > 9 {
		return false
	}
	var buf [9]byte
	upper := buf[:len(word)]
	for i := range len(word) {
		c := word[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		upper[i] = c
	}
	return keywords[string(upper)]
}

var keywords = map[string]bool{
	"ALL": true, "AND": true, "ANY": true, "ARRAY": true, "AS": true, "BETWEEN": true, "BY": true,
	"CASE": true, "CAST": true, "COLLATE": true, "DISTINCT": true, "DIV": true, "ELSE": true, "END": true,
	"ESCAPE": true, "EXISTS": true, "FALSE": true, "FROM": true, "GLOB": true, "GROUP": true, "HAVING": true,
	"ILIKE": true, "IN": true, "INTERVAL": true, "INTO": true, "IS": true, "JOIN": true, "LIKE": true,
	"LIMIT": true, "MOD": true, "NOT": true, "NULL": true, "OFFSET": true, "ON": true, "OR": true,
	"ORDER": true, "REGEXP": true, "RETURNING": true, "RLIKE": true, "ROW": true, "SELECT": true, "SET": true,
	"SIMILAR": true, "SOME": true, "SYMMETRIC": true, "THEN": true, "TO": true, "TRUE": true, "UNKNOWN": true,
	"USING": true, "VALUE": true, "VALUES": true, "WHEN": true, "WHERE": true, "WITH": true,
}

package report

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
)

// maxDatagram is the most one UDP datagram carries: under the common path
// MTU of 1500 with headers, and under every StatsD server's read buffer.
const maxDatagram = 1400

// statsd sends the bundle's facts as gauges over UDP, Datadog-style tags.
// A field absent from the bundle sends no gauge.
type statsd struct {
	conn net.Conn
}

func newStatsD(addr string) (*statsd, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", addr, err)
	}
	return &statsd{conn: conn}, nil
}

func (s *statsd) close() { _ = s.conn.Close() }

// send writes every gauge for one database's section. A write that fails
// is dropped: UDP has no answer to give, and the next interval resends.
func (s *statsd) send(db string, d *wire.Database) {
	for _, gram := range datagrams(db, d) {
		_, _ = s.conn.Write([]byte(gram))
	}
}

// datagrams is the gauges for one section, packed into datagrams of at
// most maxDatagram bytes, each a whole number of lines.
func datagrams(db string, d *wire.Database) []string {
	var out []string
	var b strings.Builder
	base := "db:" + db
	g := func(name string, v any, tags string) {
		line := fmt.Sprintf("dbhealth.%s:%s|g|#%s\n", name, gauge(v), tags)
		if b.Len()+len(line) > maxDatagram && b.Len() > 0 {
			out = append(out, b.String())
			b.Reset()
		}
		b.WriteString(line)
	}
	for i := range catalog {
		f := &catalog[i]
		if f.StatsD == "" {
			continue
		}
		for _, sm := range f.samples(d, time.Time{}) {
			tags := base
			if f.label != "" {
				tags += "," + f.label + ":" + sm.label
			}
			g(f.StatsD, sm.value, tags)
		}
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

// gauge formats a value as StatsD reads it: plain decimal, never
// scientific, and never negative, which a server reads as a decrement
// rather than a value. Replica lag can read below zero on clock skew.
func gauge(v any) string {
	switch n := v.(type) {
	case float64:
		if n < 0 {
			n = 0
		}
		return strconv.FormatFloat(n, 'f', -1, 64)
	case int64:
		if n < 0 {
			n = 0
		}
		return strconv.FormatInt(n, 10)
	}
	return fmt.Sprint(v)
}

package report

import (
	"fmt"
	"net"
	"strings"

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
		line := fmt.Sprintf("dbhealth.%s:%v|g|#%s\n", name, v, tags)
		if b.Len()+len(line) > maxDatagram && b.Len() > 0 {
			out = append(out, b.String())
			b.Reset()
		}
		b.WriteString(line)
	}
	g("probe.ok", boolInt(d.Probe.OK), base)
	g("probe.latency_ms", d.Probe.LatencyMs, base)
	g("probe.consecutive_failures", d.Probe.ConsecutiveFailures, base)
	if r := d.Resources; r != nil {
		if c := r.Connections; c != nil {
			g("connections.used", c.Used, base)
			g("connections.max", c.Max, base)
			g("connections.waiting", c.Waiting, base)
		}
		if r.SizeBytes != nil {
			g("size_bytes", *r.SizeBytes, base)
		}
		if r.OldestTransactionSeconds != nil {
			g("oldest_transaction_seconds", *r.OldestTransactionSeconds, base)
		}
		if r.Memory != nil {
			g("memory.shared_buffers_bytes", r.Memory.SharedBuffersBytes, base)
		}
	}
	for _, t := range d.Tables {
		tags := base + ",table:" + t.Name
		if t.Rows != nil {
			g("table.rows", *t.Rows, tags)
			g("table.rows_exact", boolInt(t.RowsExact), tags)
		}
		g("table.size_bytes", t.SizeBytes, tags)
		if t.NewestAt != nil {
			g("table.newest_at", t.NewestAt.Unix(), tags)
		}
	}
	if r := d.Replication; r != nil {
		if r.LagSeconds != nil {
			g("replication.lag_seconds", *r.LagSeconds, base)
		}
		for _, rep := range r.Replicas {
			if rep.LagSeconds != nil {
				g("replication.replica.lag_seconds", *rep.LagSeconds, base+",replica:"+rep.Name)
			}
		}
	}
	g("collection.queries", d.Collection.Queries, base)
	g("collection.duration_ms", d.Collection.DurationMs, base)
	g("collection.errors", len(d.Collection.Errors), base)
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

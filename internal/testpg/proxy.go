package testpg

import (
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"
)

const chunkSize = 32 * 1024

// Proxy is a TCP proxy that simulates a wide-area link: a delay (plus jitter) per direction, cuts
// (all connections closed, new ones refused for a while) and random connection drops.
type Proxy struct {
	target string
	ln     net.Listener

	mu        sync.Mutex
	latency   time.Duration
	jitter    time.Duration
	dropEvery time.Duration // mean time between random drops of a connection; 0 = never
	cutUntil  time.Time
	conns     map[net.Conn]bool
	closed    bool
}

// NewProxy listens on 127.0.0.1 and forwards to target ("host:port").
func NewProxy(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{target: target, ln: ln, conns: map[net.Conn]bool{}}
	go p.accept()
	return p, nil
}

// Addr is the listening address of the proxy.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// DSN returns dsn with its host:port replaced by the proxy's address.
func (p *Proxy) DSN(dsn string) string { return strings.Replace(dsn, p.target, p.Addr(), 1) }

// SetLink sets the one-way delay, its jitter and the mean time between random drops (0 = none).
func (p *Proxy) SetLink(latency, jitter, dropEvery time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latency, p.jitter, p.dropEvery = latency, jitter, dropEvery
}

// Cut closes every open connection and refuses new ones for d.
func (p *Proxy) Cut(d time.Duration) {
	p.mu.Lock()
	p.cutUntil = time.Now().Add(d)
	conns := p.conns
	p.conns = map[net.Conn]bool{}
	p.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// Close stops the proxy and closes all connections.
func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.ln.Close()
	p.Cut(0)
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *Proxy) link() (time.Duration, time.Duration, time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latency, p.jitter, p.dropEvery, time.Now().Before(p.cutUntil) || p.closed
}

func (p *Proxy) track(cs ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || time.Now().Before(p.cutUntil) {
		return false
	}
	for _, c := range cs {
		p.conns[c] = true
	}
	return true
}

func (p *Proxy) handle(client net.Conn) {
	if _, _, _, cut := p.link(); cut {
		_ = client.Close()
		return
	}
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	if !p.track(client, server) {
		_ = client.Close()
		_ = server.Close()
		return
	}
	closeBoth := sync.OnceFunc(func() { _ = client.Close(); _ = server.Close() })
	go p.pump(client, server, closeBoth)
	go p.pump(server, client, closeBoth)
	if _, _, every, _ := p.link(); every > 0 {
		go func() {
			time.Sleep(time.Duration(rand.Int64N(int64(2 * every))))
			closeBoth()
		}()
	}
}

type chunk struct {
	data []byte
	due  time.Time
}

// pump copies src to dst, delivering every chunk after the configured delay; chunks keep their order.
func (p *Proxy) pump(src, dst net.Conn, closeBoth func()) {
	q := make(chan chunk, 1024)
	go func() {
		defer close(q)
		var last time.Time
		buf := make([]byte, chunkSize)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				lat, jit, _, _ := p.link()
				due := time.Now().Add(lat)
				if jit > 0 {
					due = due.Add(time.Duration(rand.Int64N(int64(2*jit))) - jit)
				}
				if due.Before(last) {
					due = last // never reorder
				}
				last = due
				q <- chunk{data: append([]byte(nil), buf[:n]...), due: due}
			}
			if err != nil {
				return
			}
		}
	}()
	defer closeBoth()
	for c := range q {
		if d := time.Until(c.due); d > 0 {
			time.Sleep(d)
		}
		if _, err := dst.Write(c.data); err != nil {
			return
		}
	}
	if tc, ok := dst.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
	_, _ = io.Copy(io.Discard, src)
}

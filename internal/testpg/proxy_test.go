package testpg

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						_, _ = c.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func TestProxy(t *testing.T) {
	p, err := NewProxy(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	t.Run("P1_round_trip_has_twice_the_latency", func(t *testing.T) {
		p.SetLink(50*time.Millisecond, 0, 0)
		c, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		t0 := time.Now()
		if _, err := c.Write([]byte("ping\n")); err != nil {
			t.Fatal(err)
		}
		if _, err := bufio.NewReader(c).ReadString('\n'); err != nil {
			t.Fatal(err)
		}
		if rtt := time.Since(t0); rtt < 100*time.Millisecond || rtt > 300*time.Millisecond {
			t.Errorf("rtt = %s, want ≈ 100 ms", rtt)
		}
	})

	t.Run("P2_order_kept_with_jitter", func(t *testing.T) {
		p.SetLink(5*time.Millisecond, 5*time.Millisecond, 0)
		c, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		go func() {
			for i := 0; i < 500; i++ {
				fmt.Fprintf(c, "%d\n", i)
			}
		}()
		r := bufio.NewReader(c)
		for i := 0; i < 500; i++ {
			line, err := r.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if line != fmt.Sprintf("%d\n", i) {
				t.Fatalf("got %q at %d: order lost", line, i)
			}
		}
	})

	t.Run("P3_cut_closes_and_refuses", func(t *testing.T) {
		p.SetLink(0, 0, 0)
		c, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		time.Sleep(50 * time.Millisecond)
		p.Cut(300 * time.Millisecond)
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Error("connection still open after cut")
		}
		c2, err := net.Dial("tcp", p.Addr())
		if err == nil {
			_ = c2.SetReadDeadline(time.Now().Add(time.Second))
			_, rerr := c2.Read(make([]byte, 1))
			c2.Close()
			if rerr == nil {
				t.Error("new connection served during the cut")
			}
		}
		time.Sleep(400 * time.Millisecond)
		c3, err := net.Dial("tcp", p.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer c3.Close()
		fmt.Fprint(c3, "back\n")
		_ = c3.SetReadDeadline(time.Now().Add(time.Second))
		if line, err := bufio.NewReader(c3).ReadString('\n'); err != nil || line != "back\n" {
			t.Errorf("after the cut: %q %v", line, err)
		}
	})
}

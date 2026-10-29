// wsload: a cheap WebSocket echo load generator, so the server under test
// saturates before the client does.
//
//	wsload -addr 127.0.0.1:9001 -c 2000 -rate 20 -size 50 -warmup 3s -duration 10s
//
// -rate 0 is a closed loop: -inflight messages per connection, each reply
// answered at once. Prints "connected", "measuring", then one JSON line.
package main

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	addr     = flag.String("addr", "127.0.0.1:9001", "host:port")
	useTLS   = flag.Bool("tls", false, "wss")
	conns    = flag.Int("c", 500, "connections")
	rate     = flag.Float64("rate", 20, "messages per second per connection; 0 = closed loop")
	inflight = flag.Int("inflight", 1, "closed loop: messages in flight per connection")
	size     = flag.Int("size", 50, "payload bytes (>= 16)")
	warmup   = flag.Duration("warmup", 3*time.Second, "")
	duration = flag.Duration("duration", 10*time.Second, "")
	dialPar  = flag.Int("dial", 50, "connections opened at once")
	senders  = flag.Int("senders", 0, "paced mode: sending goroutines (default GOMAXPROCS)")
	spread   = flag.Bool("spread", false, "paced mode: a timer per connection at a random phase, so arrivals don't come in bursts")
)

var epoch = time.Now()

func now() int64 { return int64(time.Since(epoch)) }

var (
	measuring atomic.Bool
	running   atomic.Bool
	sent      atomic.Int64
	received  atomic.Int64
)

type conn struct {
	c    net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex
	wbuf []byte
	lat  []int32 // µs
}

var mask = [4]byte{0x12, 0x34, 0x56, 0x78}

func dial(id int) (*conn, error) {
	var c net.Conn
	var err error
	if *useTLS {
		c, err = tls.Dial("tcp", *addr, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"http/1.1"}})
	} else {
		c, err = net.Dial("tcp", *addr)
	}
	if err != nil {
		return nil, err
	}
	if t, ok := c.(*net.TCPConn); ok {
		t.SetNoDelay(true)
	}
	req := "GET /echo HTTP/1.1\r\nHost: " + *addr + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(c, req); err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(c, 64*1024)
	status, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.Contains(status, " 101 ") {
		return nil, fmt.Errorf("upgrade refused: %q", status)
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == "\r\n" {
			break
		}
	}
	return &conn{c: c, r: r, wbuf: make([]byte, 0, 14+*size)}, nil
}

// frame writes one masked client frame of `payload` under the write lock.
func (k *conn) frame(op byte, payload []byte) error {
	k.wmu.Lock()
	defer k.wmu.Unlock()
	b := k.wbuf[:0]
	b = append(b, 0x80|op)
	n := len(payload)
	switch {
	case n < 126:
		b = append(b, 0x80|byte(n))
	case n < 65536:
		b = append(b, 0x80|126, byte(n>>8), byte(n))
	default:
		b = append(b, 0x80|127)
		b = binary.BigEndian.AppendUint64(b, uint64(n))
	}
	b = append(b, mask[:]...)
	for i, x := range payload {
		b = append(b, x^mask[i&3])
	}
	k.wbuf = b
	_, err := k.c.Write(b)
	return err
}

var payloadPad []byte

func (k *conn) send() {
	p := make([]byte, *size)
	copy(p[8:], payloadPad)
	binary.LittleEndian.PutUint64(p, uint64(now()))
	if measuring.Load() {
		sent.Add(1)
	}
	if err := k.frame(2, p); err != nil && running.Load() {
		fmt.Fprintln(os.Stderr, "write:", err)
	}
}

func (k *conn) readLoop(closedLoop bool) {
	var hdr [10]byte
	buf := make([]byte, 64*1024)
	for {
		if _, err := io.ReadFull(k.r, hdr[:2]); err != nil {
			return
		}
		op := hdr[0] & 0x0f
		n := int(hdr[1] & 0x7f)
		switch n {
		case 126:
			if _, err := io.ReadFull(k.r, hdr[2:4]); err != nil {
				return
			}
			n = int(binary.BigEndian.Uint16(hdr[2:4]))
		case 127:
			if _, err := io.ReadFull(k.r, hdr[2:10]); err != nil {
				return
			}
			n = int(binary.BigEndian.Uint64(hdr[2:10]))
		}
		if n > len(buf) {
			buf = make([]byte, n)
		}
		p := buf[:n]
		if _, err := io.ReadFull(k.r, p); err != nil {
			return
		}
		switch op {
		case 9: // ping
			k.frame(10, p)
			continue
		case 8:
			return
		case 1, 2:
		default:
			continue
		}
		if n >= 8 && measuring.Load() {
			received.Add(1)
			t := int64(binary.LittleEndian.Uint64(p))
			k.lat = append(k.lat, int32((now()-t)/1000))
		}
		if closedLoop && running.Load() {
			k.send()
		}
	}
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func main() {
	flag.Parse()
	if *size < 16 {
		*size = 16
	}
	payloadPad = make([]byte, *size)
	for i := range payloadPad {
		payloadPad[i] = byte('a' + i%26)
	}

	all := make([]*conn, *conns)
	var wg sync.WaitGroup
	sem := make(chan struct{}, *dialPar)
	var failed atomic.Int64
	for i := range all {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			k, err := dial(i)
			if err != nil {
				if failed.Add(1) == 1 {
					fmt.Fprintln(os.Stderr, "dial:", err)
				}
				return
			}
			all[i] = k
		}(i)
	}
	wg.Wait()
	if failed.Load() > 0 {
		fmt.Fprintf(os.Stderr, "%d connections failed\n", failed.Load())
		os.Exit(1)
	}
	fmt.Println("connected")

	closedLoop := *rate == 0
	running.Store(true)
	var readers sync.WaitGroup
	for _, k := range all {
		readers.Add(1)
		go func(k *conn) { defer readers.Done(); k.readLoop(closedLoop) }(k)
	}

	stopSend := make(chan struct{})
	var sendWG sync.WaitGroup
	if closedLoop {
		for _, k := range all {
			for j := 0; j < *inflight; j++ {
				k.send()
			}
		}
	} else if *spread {
		interval := time.Duration(float64(time.Second) / *rate)
		for _, k := range all {
			sendWG.Add(1)
			go func(k *conn) {
				defer sendWG.Done()
				select {
				case <-stopSend:
					return
				case <-time.After(time.Duration(rand.Int63n(int64(interval)))):
				}
				t := time.NewTicker(interval)
				defer t.Stop()
				for {
					select {
					case <-stopSend:
						return
					case <-t.C:
						k.send()
					}
				}
			}(k)
		}
	} else {
		s := *senders
		if s <= 0 {
			s = runtime.GOMAXPROCS(0)
		}
		for g := 0; g < s; g++ {
			var mine []*conn
			for i := g; i < len(all); i += s {
				mine = append(mine, all[i])
			}
			if len(mine) == 0 {
				continue
			}
			sendWG.Add(1)
			go func(mine []*conn) {
				defer sendWG.Done()
				perNs := float64(len(mine)) * *rate / 1e9
				owed := 0.0
				next := 0
				last := now()
				// owed by the clock: a late wakeup must not lower the rate
				t := time.NewTicker(time.Millisecond)
				defer t.Stop()
				for {
					select {
					case <-stopSend:
						return
					case <-t.C:
					}
					n := now()
					owed += float64(n-last) * perNs
					last = n
					for owed >= 1 {
						mine[next].send()
						next = (next + 1) % len(mine)
						owed--
					}
				}
			}(mine)
		}
	}

	time.Sleep(*warmup)
	cpu0 := cpuSeconds()
	t0 := time.Now()
	measuring.Store(true)
	fmt.Println("measuring")
	time.Sleep(*duration)
	elapsed := time.Since(t0).Seconds()
	cpu := (cpuSeconds() - cpu0) / elapsed
	running.Store(false)
	close(stopSend)
	sendWG.Wait()
	// in flight still counts as received
	time.Sleep(500 * time.Millisecond)
	measuring.Store(false)

	var lat []int32
	for _, k := range all {
		k.c.Close()
	}
	readers.Wait()
	for _, k := range all {
		lat = append(lat, k.lat...)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		i := int(p * float64(len(lat)))
		if i >= len(lat) {
			i = len(lat) - 1
		}
		return float64(lat[i]) / 1000
	}
	s, r := sent.Load(), received.Load()
	out := map[string]any{
		"sent": s, "received": r,
		"sentPerSecond": float64(s) / elapsed, "receivedPerSecond": float64(r) / elapsed,
		"p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "p999": pct(0.999), "max": pct(1),
		"clientCpu": cpu,
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}

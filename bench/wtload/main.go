// wtload: a cheap WebTransport echo load generator, the wsload of
// WebTransport, so the server under test saturates before the client does.
//
//	wtload -addr 127.0.0.1:4433 -c 200 -rate 20 -size 64 -mode datagram -warmup 3s -duration 10s
//
// Each session is its own QUIC connection. -mode datagram sends each message
// as one datagram; -mode stream writes them back to back on one long-lived
// bidi stream per session. -rate 0 is a closed loop: -inflight messages per
// session, each echo answered at once (a datagram echo later than
// -losstimeout is presumed lost and replaced).
//
// Only messages sent during -duration count, toward both sent and received;
// an echo counts if it arrives before -drain ends. Prints "connected" and
// "measuring" to stdout like wsload, then one JSON line; everything else
// goes to stderr.
//
// A message is its send time (16 hex digits) and sequence number (8),
// padded to -size with lowercase letters. It never contains an 'E', so an
// "Echo: " the server prepends to whatever it read, wherever that lands in
// the stream, is recognised and skipped.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

var (
	addr      = flag.String("addr", "127.0.0.1:4433", "host:port")
	path      = flag.String("path", "/", "WebTransport endpoint path")
	sessions  = flag.Int("c", 500, "sessions, one QUIC connection each")
	rate      = flag.Float64("rate", 20, "messages per second per session; 0 = closed loop")
	inflight  = flag.Int("inflight", 1, "closed loop: messages in flight per session")
	size      = flag.Int("size", 50, "payload bytes (>= 24)")
	mode      = flag.String("mode", "datagram", "datagram, or stream (one long-lived bidi stream per session)")
	warmup    = flag.Duration("warmup", 3*time.Second, "")
	duration  = flag.Duration("duration", 10*time.Second, "")
	dialPar   = flag.Int("dial", 50, "sessions dialed at once")
	dialTO    = flag.Duration("dialtimeout", 10*time.Second, "per-session handshake + CONNECT timeout")
	senders   = flag.Int("senders", 0, "paced mode: sending goroutines (default GOMAXPROCS)")
	spread    = flag.Bool("spread", false, "paced mode: a timer per session at a random phase, so arrivals don't come in bursts")
	insecure  = flag.Bool("insecure", true, "skip certificate verification")
	lossAfter = flag.Duration("losstimeout", time.Second, "closed-loop datagrams: an echo this late is presumed lost and its slot refilled")
	drain     = flag.Duration("drain", 500*time.Millisecond, "after sending stops, how long echoes still count")
	progress  = flag.Duration("progress", 0, "print rates to stderr at this interval (0 = off)")
)

const (
	tsLen   = 16
	seqLen  = 8
	hdrLen  = tsLen + seqLen
	hexdigs = "0123456789abcdef"
)

var echoPrefix = []byte("Echo: ")

var epoch = time.Now()

func now() int64 { return int64(time.Since(epoch)) }

var (
	running     atomic.Bool
	collecting  atomic.Bool
	measureFrom atomic.Int64 // send-time window of messages that count
	measureTo   atomic.Int64
	sent        atomic.Int64
	received    atomic.Int64
	sentAll     atomic.Int64 // for -progress
	recvAll     atomic.Int64
	sawPrefix   atomic.Bool
	errCount    atomic.Int64
)

func counts(t int64) bool { return t >= measureFrom.Load() && t < measureTo.Load() }

// fail counts an error and prints the first few.
func fail(format string, args ...any) {
	if errCount.Add(1) <= 5 {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

type session struct {
	qc  *quic.Conn
	s   *webtransport.Session
	str *webtransport.Stream // stream mode

	wmu sync.Mutex // seq, buf, cur, at, and write order on str
	seq uint32
	buf []byte
	cur []uint32 // closed-loop datagrams: the seq each slot waits for
	at  []int64  // and when it went out

	lmu sync.Mutex
	lat []int32 // µs
}

var payloadPad []byte

func putHex(b []byte, v uint64) {
	for i := len(b) - 1; i >= 0; i-- {
		b[i] = hexdigs[v&15]
		v >>= 4
	}
}

func getHex(b []byte) (uint64, bool) {
	var v uint64
	for _, c := range b {
		switch {
		case c >= '0' && c <= '9':
			v = v<<4 | uint64(c-'0')
		case c >= 'a' && c <= 'f':
			v = v<<4 | uint64(c-'a'+10)
		default:
			return 0, false
		}
	}
	return v, true
}

// write sends one message with `seq`; the caller holds wmu.
func (k *session) write(seq uint32) {
	t := now()
	putHex(k.buf[:tsLen], uint64(t))
	putHex(k.buf[tsLen:hdrLen], uint64(seq))
	var err error
	if k.str != nil {
		_, err = k.str.Write(k.buf)
	} else {
		err = k.s.SendDatagram(k.buf)
	}
	if err != nil {
		if running.Load() {
			fail("send: %v", err)
		}
		return
	}
	sentAll.Add(1)
	if counts(t) {
		sent.Add(1)
	}
}

func (k *session) send() {
	k.wmu.Lock()
	seq := k.seq
	k.seq++
	k.write(seq)
	k.wmu.Unlock()
}

// sendSlot refills closed-loop datagram slot j, unless it has moved on from
// `expect` already (the echo was late and the slot refilled).
func (k *session) sendSlot(j int, expect uint32) {
	k.wmu.Lock()
	defer k.wmu.Unlock()
	if k.cur[j] != expect || !running.Load() {
		return
	}
	seq := expect + uint32(len(k.cur))
	k.cur[j] = seq
	k.at[j] = now()
	k.write(seq)
}

// echoed records an echo of the message sent at t.
func (k *session) echoed(t int64) {
	recvAll.Add(1)
	if !counts(t) || !collecting.Load() {
		return
	}
	received.Add(1)
	us := (now() - t) / 1000
	k.lmu.Lock()
	k.lat = append(k.lat, int32(min(us, math.MaxInt32)))
	k.lmu.Unlock()
}

func parse(m []byte) (t int64, seq uint32, ok bool) {
	tv, ok1 := getHex(m[:tsLen])
	sv, ok2 := getHex(m[tsLen:hdrLen])
	return int64(tv), uint32(sv), ok1 && ok2
}

func (k *session) readDatagrams(closedLoop bool) {
	for {
		d, err := k.s.ReceiveDatagram(context.Background())
		if err != nil {
			if running.Load() {
				fail("receive datagram: %v", err)
			}
			return
		}
		if bytes.HasPrefix(d, echoPrefix) {
			d = d[len(echoPrefix):]
			sawPrefix.Store(true)
		}
		if len(d) != *size {
			fail("datagram echo of %d bytes, sent %d", len(d), *size)
			continue
		}
		t, seq, ok := parse(d)
		if !ok {
			fail("datagram echo is not ours: %q", d[:hdrLen])
			continue
		}
		k.echoed(t)
		if closedLoop {
			n := uint32(len(k.cur))
			k.sendSlot(int(seq%n), seq)
		}
	}
}

// readStream reassembles the echo stream into -size messages, skipping any
// "Echo: " prefixes, and checks they come back in the order they were sent.
func (k *session) readStream(closedLoop bool) {
	buf := make([]byte, 64*1024)
	msg := make([]byte, *size)
	have := 0 // bytes of msg filled
	pfx := 0  // bytes of an "Echo: " matched
	var want uint32
	for {
		n, err := k.str.Read(buf)
		p := buf[:n]
		for len(p) > 0 {
			if pfx > 0 {
				if p[0] != echoPrefix[pfx] {
					fail("stream desync: %q where %q continues", p[0], echoPrefix)
					return
				}
				p = p[1:]
				if pfx++; pfx == len(echoPrefix) {
					pfx = 0
				}
				continue
			}
			if p[0] == 'E' {
				sawPrefix.Store(true)
				pfx = 1
				p = p[1:]
				continue
			}
			take := min(len(p), *size-have)
			if i := bytes.IndexByte(p[:take], 'E'); i >= 0 {
				take = i
			}
			copy(msg[have:], p[:take])
			have += take
			p = p[take:]
			if have < *size {
				continue
			}
			have = 0
			t, seq, ok := parse(msg)
			if !ok || seq != want {
				// the server dropped or reordered bytes; nothing after this lines up
				fail("stream desync: expected seq %d, got %q", want, msg[:hdrLen])
				return
			}
			want++
			k.echoed(t)
			if closedLoop && running.Load() {
				k.send()
			}
		}
		if err != nil {
			if running.Load() {
				fail("stream read: %v", err)
			}
			return
		}
	}
}

func dial(tlsConf *tls.Config, quicConf *quic.Config, url string) (*session, error) {
	k := &session{buf: make([]byte, *size)}
	d := &webtransport.Dialer{
		TLSClientConfig: tlsConf,
		QUICConfig:      quicConf,
		// kept so closeAll can wait for the CONNECTION_CLOSE to go out
		DialAddr: func(ctx context.Context, addr string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
			c, err := quic.DialAddrEarly(ctx, addr, tc, qc)
			k.qc = c
			return c, err
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), *dialTO)
	defer cancel()
	_, s, err := d.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	k.s = s
	copy(k.buf[hdrLen:], payloadPad[hdrLen:])
	if *mode == "stream" {
		if k.str, err = s.OpenStreamSync(ctx); err != nil {
			s.CloseWithError(0, "")
			return nil, fmt.Errorf("open stream: %w", err)
		}
	}
	return k, nil
}

func closeAll(all []*session) {
	var wg sync.WaitGroup
	for _, k := range all {
		if k == nil {
			continue
		}
		wg.Add(1)
		go func(k *session) {
			defer wg.Done()
			k.s.CloseWithError(0, "done")
			// the session closes its connection from another goroutine; an exit can beat it
			k.qc.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
		}(k)
	}
	closed := make(chan struct{})
	go func() { wg.Wait(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		fmt.Fprintln(os.Stderr, "some sessions did not close within 3s")
	}
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func main() {
	flag.Parse()
	if *mode != "datagram" && *mode != "stream" {
		fmt.Fprintln(os.Stderr, "-mode must be datagram or stream")
		os.Exit(2)
	}
	if *size < hdrLen {
		*size = hdrLen
	}
	*inflight = max(*inflight, 1)
	payloadPad = make([]byte, *size)
	for i := range payloadPad {
		payloadPad[i] = byte('a' + i%26)
	}
	measureFrom.Store(math.MaxInt64)
	measureTo.Store(math.MaxInt64)

	url := "https://" + *addr + *path
	tlsConf := &tls.Config{InsecureSkipVerify: *insecure}
	quicConf := &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true, // webtransport-go refuses to dial without it
		MaxIdleTimeout:                   30 * time.Second,
		KeepAlivePeriod:                  10 * time.Second,
	}
	fmt.Fprintf(os.Stderr, "dialing %d sessions to %s (%s)\n", *sessions, url, *mode)

	all := make([]*session, *sessions)
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(*dialPar, 1))
	var failed atomic.Int64
	var firstErr atomic.Value
	t0dial := time.Now()
	for i := range all {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			k, err := dial(tlsConf, quicConf, url)
			if err != nil {
				if failed.Add(1) == 1 {
					firstErr.Store(err)
				}
				return
			}
			all[i] = k
		}(i)
	}
	wg.Wait()
	if n := failed.Load(); n > 0 {
		fmt.Fprintf(os.Stderr, "%d of %d sessions failed; first: %v\n", n, *sessions, firstErr.Load())
		closeAll(all) // unlike TCP, an exit leaves the server holding them until idle timeout
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "%d sessions up in %v\n", *sessions, time.Since(t0dial).Round(time.Millisecond))
	fmt.Println("connected")

	closedLoop := *rate == 0
	running.Store(true)
	collecting.Store(true)
	for _, k := range all {
		if k.str != nil {
			go k.readStream(closedLoop)
		} else {
			if closedLoop {
				k.cur = make([]uint32, *inflight)
				k.at = make([]int64, *inflight)
				for j := range k.cur {
					k.cur[j] = uint32(j) - uint32(*inflight) // first sendSlot lands on seq j
				}
			}
			go k.readDatagrams(closedLoop)
		}
	}

	stopSend := make(chan struct{})
	var sendWG sync.WaitGroup
	if closedLoop {
		for _, k := range all {
			if k.str != nil {
				for j := 0; j < *inflight; j++ {
					k.send()
				}
				continue
			}
			for j := range k.cur {
				k.sendSlot(j, k.cur[j])
			}
		}
		if *mode == "datagram" {
			sendWG.Add(1)
			go func() {
				defer sendWG.Done()
				t := time.NewTicker(max(*lossAfter/4, 10*time.Millisecond))
				defer t.Stop()
				for {
					select {
					case <-stopSend:
						return
					case <-t.C:
					}
					old := now() - int64(*lossAfter)
					for _, k := range all {
						for j := range k.cur {
							k.wmu.Lock()
							stale, seq := k.at[j] < old, k.cur[j]
							k.wmu.Unlock()
							if stale {
								k.sendSlot(j, seq)
							}
						}
					}
				}
			}()
		}
	} else if *spread {
		interval := time.Duration(float64(time.Second) / *rate)
		for _, k := range all {
			sendWG.Add(1)
			go func(k *session) {
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
			var mine []*session
			for i := g; i < len(all); i += s {
				mine = append(mine, all[i])
			}
			if len(mine) == 0 {
				continue
			}
			sendWG.Add(1)
			go func(mine []*session) {
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

	if *progress > 0 {
		go func() {
			t := time.NewTicker(*progress)
			defer t.Stop()
			ps, pr, pt := sentAll.Load(), recvAll.Load(), time.Now()
			for range t.C {
				s, r, n := sentAll.Load(), recvAll.Load(), time.Now()
				dt := n.Sub(pt).Seconds()
				fmt.Fprintf(os.Stderr, "sent %.0f/s, echoed %.0f/s, errors %d\n", float64(s-ps)/dt, float64(r-pr)/dt, errCount.Load())
				ps, pr, pt = s, r, n
			}
		}()
	}

	time.Sleep(*warmup)
	cpu0 := cpuSeconds()
	t0 := time.Now()
	measureFrom.Store(now())
	fmt.Println("measuring")
	time.Sleep(*duration)
	measureTo.Store(now())
	elapsed := time.Since(t0).Seconds()
	cpu := (cpuSeconds() - cpu0) / elapsed
	running.Store(false)
	close(stopSend)
	sendWG.Wait()
	time.Sleep(*drain)
	collecting.Store(false)

	var lat []int32
	silent := 0 // sessions the server stopped answering, or never did
	for _, k := range all {
		k.lmu.Lock()
		if len(k.lat) == 0 {
			silent++
		}
		lat = append(lat, k.lat...)
		k.lmu.Unlock()
	}
	closeAll(all)

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
	loss := 0.0
	if s > 0 {
		loss = float64(s-r) / float64(s)
	}
	out := map[string]any{
		"sent": s, "received": r,
		"sentPerSecond": float64(s) / elapsed, "receivedPerSecond": float64(r) / elapsed,
		"p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "p999": pct(0.999), "max": pct(1),
		"clientCpu": cpu, "mode": *mode, "sessions": *sessions, "size": *size,
		"lost": s - r, "loss": loss, "errors": errCount.Load(), "silent": silent,
		"echoPrefix": sawPrefix.Load(),
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}

// quic-go's WebTransport echo, for comparing with netcode-io under wtload:
//   go build -o wtecho ./echo && ./wtecho -addr 127.0.0.1:4435 -q
// (from the repo root, for test/certs).
//
// Raw echo WebTransport server for benchmarking.
// - Bidi streams: io.Copy (no prefix, keeps stream alive)
// - Datagrams: echo exact bytes back
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

var sessionCount atomic.Int64

func main() {
	addr := flag.String("addr", "0.0.0.0:4435", "listen address")
	certFile := flag.String("cert", "test/certs/server.crt", "TLS certificate")
	keyFile := flag.String("key", "test/certs/server.key", "TLS private key")
	quiet := flag.Bool("q", false, "suppress per-session logging")
	flag.Parse()

	if *quiet {
		log.SetOutput(io.Discard)
	} else {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	}

	h3Server := &http3.Server{
		Addr: *addr,
		TLSConfig: &tls.Config{
			NextProtos: []string{http3.NextProtoH3},
		},
	}
	webtransport.ConfigureHTTP3Server(h3Server)

	s := webtransport.Server{
		H3:          h3Server,
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		sess, err := s.Upgrade(w, r)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		n := sessionCount.Add(1)
		log.Printf("session #%d established from %s", n, r.RemoteAddr)
		go handleSession(sess)
	})

	fmt.Fprintf(os.Stderr, "quic-go echo server listening on %s\n", *addr)
	if err := s.ListenAndServeTLS(*certFile, *keyFile); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func handleSession(sess *webtransport.Session) {
	ctx := context.Background()

	// Bidi streams: raw echo via io.Copy
	go func() {
		for {
			stream, err := sess.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() {
				io.Copy(stream, stream)
			}()
		}
	}()

	// Datagrams: raw echo
	go func() {
		for {
			data, err := sess.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			sess.SendDatagram(data)
		}
	}()

	<-ctx.Done()
}

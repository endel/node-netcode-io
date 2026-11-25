//go:build !windows

package main

import "time"

var epoch = time.Now()

// now is nanoseconds on a monotonic clock, for round trips.
func now() int64 { return int64(time.Since(epoch)) }

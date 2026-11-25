package main

import (
	"syscall"
	"unsafe"
)

// Go's monotonic clock on Windows advances once per timer interrupt, every
// 0.5 to 15.6 ms: a loopback round trip reads as 0. QueryPerformanceCounter
// counts in 100 ns steps or finer.
var qpc = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceCounter")

var qpcHz = func() int64 {
	var hz int64
	syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&hz)))
	return hz
}()

var epoch = ticks()

func ticks() int64 {
	var t int64
	qpc.Call(uintptr(unsafe.Pointer(&t)))
	return t
}

// now is nanoseconds on a monotonic clock, for round trips.
func now() int64 {
	t := ticks() - epoch
	return t/qpcHz*1e9 + t%qpcHz*1e9/qpcHz
}

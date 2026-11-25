package main

import (
	"runtime"
	"syscall"
	"unsafe"
)

var (
	kernel32              = syscall.NewLazyDLL("kernel32.dll")
	queryProcessCycleTime = kernel32.NewProc("QueryProcessCycleTime")
	queryThreadCycleTime  = kernel32.NewProc("QueryThreadCycleTime")
	getCurrentThread      = kernel32.NewProc("GetCurrentThread")
)

// serverCpu starts counting the CPU process `pid` uses, every thread; the
// function it returns reads the seconds since. Windows charges CPU time
// (GetProcessTimes, so process.cpuUsage) a clock tick at a time, off by 5
// to 15% for a server that wakes for short bursts. Cycle time is exact: TSC
// cycles while its threads ran.
func serverCpu(pid int) func() (float64, bool) {
	const queryLimited = 0x1000 // PROCESS_QUERY_LIMITED_INFORMATION
	none := func() (float64, bool) { return 0, false }
	if pid == 0 {
		return none
	}
	h, err := syscall.OpenProcess(queryLimited, false, uint32(pid))
	if err != nil {
		return none
	}
	read := func() uint64 {
		var c uint64
		queryProcessCycleTime.Call(uintptr(h), uintptr(unsafe.Pointer(&c)))
		return c
	}
	start := read()
	return func() (float64, bool) {
		defer syscall.CloseHandle(h)
		return float64(read()-start) / tscHz(), true
	}
}

// tscHz: cycles a busy thread counts per second.
func tscHz() float64 {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	thread, _, _ := getCurrentThread.Call()
	cycles := func() uint64 {
		var c uint64
		queryThreadCycleTime.Call(thread, uintptr(unsafe.Pointer(&c)))
		return c
	}
	c0, t0 := cycles(), now()
	for now()-t0 < 200e6 {
	}
	return float64(cycles()-c0) / (float64(now()-t0) / 1e9)
}

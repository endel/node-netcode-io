package main

import "syscall"

// cpuSeconds is the CPU this process has used, user and system.
func cpuSeconds() float64 {
	var created, exited, kernel, user syscall.Filetime
	h, _ := syscall.GetCurrentProcess()
	syscall.GetProcessTimes(h, &created, &exited, &kernel, &user)
	return float64(filetime(kernel)+filetime(user)) / 1e7
}

// filetime reads a duration Filetime: 100 ns units. Its Nanoseconds() is for
// instants, counted from 1601, and would subtract the Unix epoch.
func filetime(ft syscall.Filetime) uint64 {
	return uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime)
}

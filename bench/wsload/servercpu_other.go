//go:build !windows

package main

// serverCpu: elsewhere the server's own count is exact enough.
func serverCpu(pid int) func() (float64, bool) {
	return func() (float64, bool) { return 0, false }
}

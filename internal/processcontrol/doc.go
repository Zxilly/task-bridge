// Package processcontrol manages an ordinary child process tree. On Windows it
// assigns a suspended process to a kill-on-close job before allowing execution;
// on Unix it creates a process group and signals that group on termination.
// Callers own Wait and must close a prepared Control on every path.
// PrepareForParentExit additionally binds a CLI to its worker's lifetime on
// Windows and Linux. Its Start and Close must run on the same goroutine; Linux
// pins that goroutine's OS thread until Close, including when Start fails.
package processcontrol

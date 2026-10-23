// Package processcontrol manages an ordinary child process tree. On Windows it
// assigns a suspended process to a kill-on-close job before allowing execution;
// on Unix it creates a process group and signals that group on termination.
// Callers own Wait and must close a prepared Control on every path.
package processcontrol

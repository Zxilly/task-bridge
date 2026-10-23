//go:build !windows

package storage

import "golang.org/x/sys/unix"

func freeBytes(path string) (uint64, error) {
	var info unix.Statfs_t
	e := unix.Statfs(path, &info)
	return uint64(info.Bavail) * uint64(info.Bsize), e
}

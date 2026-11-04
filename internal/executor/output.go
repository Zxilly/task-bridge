package executor

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// commandOutput tracks output completion separately from process completion.
// Files passed directly to exec.Cmd keep its Wait independent of reader EOF.
type commandOutput struct {
	readers []*os.File
	writers []*os.File
	done    chan struct{}
}

func newCommandOutput(cmd *exec.Cmd, output *collector) (*commandOutput, error) {
	c := &commandOutput{done: make(chan struct{})}
	for range 2 {
		r, w, err := os.Pipe()
		if err != nil {
			for _, f := range c.readers {
				_ = f.Close()
			}
			c.closeWriters()
			return nil, err
		}
		c.readers = append(c.readers, r)
		c.writers = append(c.writers, w)
	}
	cmd.Stdout = c.writers[0]
	cmd.Stderr = c.writers[1]
	var wg sync.WaitGroup
	for i, stream := range []string{"stdout", "stderr"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := io.Copy(streamWriter{output, stream}, c.readers[i])
			if err != nil && !errors.Is(err, os.ErrClosed) {
				output.setError(err)
				output.cancel()
			}
		}()
	}
	go func() { wg.Wait(); close(c.done) }()
	return c, nil
}

func (c *commandOutput) closeWriters() {
	for _, f := range c.writers {
		_ = f.Close()
	}
}
func (c *commandOutput) close() {
	c.closeWriters()
	for _, f := range c.readers {
		_ = f.Close()
	}
	<-c.done
}

// A known shell exit stays known even if descendants keep a stream open.
// Preserve all output collected so far and tell callers the capture ended early.
func (c *commandOutput) drain(timeout time.Duration) bool {
	if c.wait(timeout) {
		return false
	}
	for _, f := range c.readers {
		_ = f.Close()
	}
	<-c.done
	return true
}

// wait leaves live readers attached when it expires, allowing background polling.
func (c *commandOutput) wait(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return true
	case <-timer.C:
		select {
		case <-c.done:
			return true
		default:
		}
		return false
	}
}

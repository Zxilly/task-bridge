//go:build windows

package processcontrol

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type Control struct {
	job      windows.Handle
	thread   windows.Handle
	assigned bool
}

// x/sys provides the process and thread handles, but does not yet wrap this
// public Win32 ownership query. Resolve it from the system DLL, not PATH.
var getProcessIDOfThread = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetProcessIdOfThread")

func Prepare(cmd *exec.Cmd) (*Control, error) {
	if err := getProcessIDOfThread.Find(); err != nil {
		return nil, fmt.Errorf("load execution thread ownership API: %w", err)
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create execution job: %w", err)
	}
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		_ = windows.CloseHandle(job)
		return nil, fmt.Errorf("configure execution job: %w", err)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED}
	return &Control{job: job}, nil
}

func (p *Control) Start(cmd *exec.Cmd) (err error) {
	if err = cmd.Start(); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			p.Terminate(cmd, true)
			_ = cmd.Wait()
		}
	}()
	var assignmentErr error
	if err = cmd.Process.WithHandle(func(handle uintptr) {
		assignmentErr = windows.AssignProcessToJobObject(p.job, windows.Handle(handle))
	}); err != nil {
		return fmt.Errorf("access execution process: %w", err)
	}
	if assignmentErr != nil {
		return fmt.Errorf("assign execution job: %w", assignmentErr)
	}
	p.assigned = true
	// Go closes CreateProcess's primary thread handle before Start returns.
	// The suspended process has not run user code; locate its initial thread
	// through the public Toolhelp API and retain that handle until journaled.
	p.thread, err = openInitialThread(uint32(cmd.Process.Pid))
	return err
}

func openInitialThread(pid uint32) (thread windows.Handle, err error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return 0, fmt.Errorf("snapshot execution thread: %w", err)
	}
	defer windows.CloseHandle(snapshot)
	defer func() {
		if err != nil && thread != 0 {
			_ = windows.CloseHandle(thread)
			thread = 0
		}
	}()
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		if thread != 0 {
			return thread, errors.New("suspended execution has multiple initial threads")
		}
		thread, err = windows.OpenThread(windows.THREAD_SUSPEND_RESUME|windows.THREAD_QUERY_LIMITED_INFORMATION, false, entry.ThreadID)
		if err != nil {
			return 0, fmt.Errorf("open execution thread: %w", err)
		}
		owner, _, queryErr := getProcessIDOfThread.Call(uintptr(thread))
		if owner == 0 {
			if queryErr == windows.ERROR_SUCCESS {
				queryErr = windows.ERROR_INVALID_HANDLE
			}
			return thread, fmt.Errorf("query execution thread owner: %w", queryErr)
		}
		if uint32(owner) != pid {
			return thread, errors.New("execution thread owner changed during startup")
		}
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return thread, fmt.Errorf("enumerate execution thread: %w", err)
	}
	if thread == 0 {
		return 0, errors.New("suspended execution thread was not found")
	}
	return thread, nil
}

func (p *Control) Resume() error {
	defer p.closeThread()
	previous, err := windows.ResumeThread(p.thread)
	if err != nil {
		return fmt.Errorf("resume execution thread: %w", err)
	}
	if previous != 1 {
		return fmt.Errorf("unexpected execution thread suspend count: %d", previous)
	}
	return nil
}

func (p *Control) Terminate(cmd *exec.Cmd, _ bool) {
	if p.job != 0 {
		if err := windows.TerminateJobObject(p.job, 1); err != nil {
			// The last non-inherited job handle also terminates its members.
			p.closeJob()
		}
	}
	if !p.assigned && cmd.Process != nil {
		// Assignment failure leaves only the suspended initial process.
		_ = cmd.Process.Kill()
	}
}

func (p *Control) closeThread() {
	if p.thread != 0 {
		_ = windows.CloseHandle(p.thread)
		p.thread = 0
	}
}

func (p *Control) closeJob() {
	if p.job != 0 {
		_ = windows.CloseHandle(p.job)
		p.job = 0
	}
}

func (p *Control) Close() {
	p.closeJob()
	p.closeThread()
}
func ExitSignal(state *os.ProcessState) string { return "" }

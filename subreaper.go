package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// Built-in `overseer subreaper` — a mini-init that wraps one agent invocation so
// nothing it spawns can leak. The jail gives the harness a fresh user+mount
// namespace but NOT a PID namespace, so a background process the harness forks
// (a dev server, a language server, a test daemon) survives the harness and
// reparents to init, accumulating across a long run. The subreaper sits as the
// immediate parent of the wrapped command and:
//
//   - marks itself a child subreaper (PR_SET_CHILD_SUBREAPER), so every orphaned
//     descendant — including setsid-detached daemons — reparents to it instead of
//     to init;
//   - reaps those orphans as they exit, keeping the process table free of zombies;
//   - when the tracked (direct) child exits, SIGKILLs whatever is left of the
//     subtree before propagating the child's exit code.
//
// CLI: overseer subreaper <cmd> [args...] — everything after the keyword is the
// command. No `--` separator: a nested jail `--` belongs to the jail and is
// passed straight through. It is the OUTERMOST wrapper — the final agent exec is
//   overseer subreaper  overseer jail --rw=… --  <harness> <harness-args…>
// composed by wrapSubreaper(wrapJail(...)).
func subreaperMain(args []string) {
	if len(args) == 0 {
		ThrowFmt("subreaper: missing command (usage: subreaper CMD [ARGS...])")
	}

	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(1), 0, 0, 0); err != nil {
		ThrowFmt("subreaper: PR_SET_CHILD_SUBREAPER: %v", err)
	}

	bin, err := exec.LookPath(args[0])

	if err != nil {
		ThrowFmt("subreaper: lookup %s: %v", args[0], err)
	}

	cmd := exec.Command(bin, args[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	// Backstop: if the subreaper itself dies, drag the tracked child down too.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

	Throw(cmd.Start())

	child := cmd.Process.Pid

	// Forward graceful termination signals to the tracked child (best-effort).
	// SIGKILL can't be caught, so an orchestrator group-kill bypasses this and
	// takes us down with the group — that's the intended force path.
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	go func() {
		for s := range sigs {
			if sg, ok := s.(syscall.Signal); ok {
				_ = syscall.Kill(child, sg)
			}
		}
	}()

	code := subreaperWait(child)
	subreaperKillTree()

	os.Exit(code)
}

// subreaperWait blocks reaping ANY child until the tracked child exits, then
// returns its exit code (128+signal when it was killed by a signal). Orphan
// zombies reaped along the way are what keeps the process table clean.
func subreaperWait(child int) int {
	for {
		var ws syscall.WaitStatus

		pid, err := syscall.Wait4(-1, &ws, 0, nil)

		if err == syscall.EINTR {
			continue
		}

		// No children left at all — the tracked child is already gone.
		if err == syscall.ECHILD {
			return 0
		}

		if err != nil {
			return 1
		}

		if pid == child {
			return waitStatusCode(ws)
		}

		// Reaped an orphan zombie — keep waiting for the tracked child.
	}
}

func waitStatusCode(ws syscall.WaitStatus) int {
	switch {
	case ws.Exited():
		return ws.ExitStatus()
	case ws.Signaled():
		return 128 + int(ws.Signal())
	}

	return 0
}

// subreaperKillTree SIGKILLs every remaining descendant after the tracked child
// has exited. Because we are the child subreaper, any orphan (including a
// setsid-detached daemon that left the original process group) reparents to us —
// so scanning /proc for processes whose ppid is us, killing them, and looping
// until none remain catches the whole subtree, not merely the process group. A
// blocking wait between scans drains the just-killed processes (their own
// children then reparent to us and surface on the next scan) without busy-spin.
func subreaperKillTree() {
	self := os.Getpid()

	for {
		kids := childPids(self)

		for _, pid := range kids {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}

		var ws syscall.WaitStatus

		_, err := syscall.Wait4(-1, &ws, 0, nil)

		if err == syscall.ECHILD {
			return
		}

		// EINTR or a reaped child: loop and re-scan for newly-reparented orphans.
	}
}

// childPids returns the pids of every process whose parent is `parent`, read
// straight from /proc. Best-effort — a process that exits mid-scan just drops out.
func childPids(parent int) []int {
	entries, err := os.ReadDir("/proc")

	if err != nil {
		return nil
	}

	var out []int

	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())

		if err != nil {
			continue
		}

		if ppidOf(pid) == parent {
			out = append(out, pid)
		}
	}

	return out
}

// ppidOf reads the parent pid out of /proc/<pid>/stat. The comm field (field 2)
// is wrapped in parens and may itself contain spaces and ')', so the fields after
// it are located from the LAST ')': state, then ppid. -1 if unreadable.
func ppidOf(pid int) int {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))

	if err != nil {
		return -1
	}

	s := string(data)
	rparen := strings.LastIndexByte(s, ')')

	if rparen < 0 {
		return -1
	}

	fields := strings.Fields(s[rparen+1:])

	if len(fields) < 2 {
		return -1
	}

	ppid, err := strconv.Atoi(fields[1])

	if err != nil {
		return -1
	}

	return ppid
}

var (
	selfExeOnce sync.Once
	selfExePath string
)

// selfExe is the absolute path to this overseer binary, cached. Used to compose
// the `overseer subreaper …` / `overseer jail …` self-recursing wrappers.
func selfExe() string {
	selfExeOnce.Do(func() {
		p, err := os.Executable()

		if err != nil {
			ThrowFmt("subreaper: os.Executable: %v", err)
		}

		selfExePath = p
	})

	return selfExePath
}

// wrapSubreaper makes the agent exec recurse through `overseer subreaper` when
// enabled, so the harness runs under a reaping mini-init. It is the outermost
// layer — call it on the (bin, args) wrapJail already produced. A no-op when
// disabled (--no-subreaper).
func wrapSubreaper(enabled bool, bin string, args []string) (string, []string) {
	if !enabled {
		return bin, args
	}

	out := append([]string{"subreaper", bin}, args...)

	return selfExe(), out
}

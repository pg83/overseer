package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// The orchestrator process makes ITSELF a "child subreaper" (PR_SET_CHILD_SUBREAPER)
// at startup, so every process spawned anywhere beneath it — a harness, something the
// harness forks, a daemon that double-forks to detach — reparents to the orchestrator
// instead of to init when its own parent exits. The jail gives each harness a fresh
// user+mount namespace but NOT a PID namespace, so without this such strays escape to
// init and leak past the run. On exit (normal, GOALS_ACHIEVED, signal, or fatal) the
// orchestrator SIGKILLs its whole descendant subtree, so nothing it ever started
// outlives it. No per-agent wrappers, process groups, or registries — one flag on the
// parent and one sweep at the end.

// becomeChildSubreaper marks this process so orphaned descendants reparent to it
// rather than to init. Best-effort: a failure only weakens the exit-time sweep (some
// re-parented grandchildren may escape), so it warns rather than aborting.
func becomeChildSubreaper() {
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(1), 0, 0, 0); err != nil {
		uiSys("⚠️", "SUBREAPER", "PR_SET_CHILD_SUBREAPER failed: "+err.Error())
	}
}

// killAllDescendants SIGKILLs every process still parented to us and reaps it, looping
// until none remain. Because we are the child subreaper, a grandchild orphaned when its
// own parent dies reparents to us and shows up on the next scan — so this tears down the
// entire subtree (setsid-detached daemons included), not just direct children. Bounded
// (~4s) so a stuck uninterruptible process can't hang shutdown forever.
func killAllDescendants() {
	self := os.Getpid()

	for round := 0; round < 200; round++ {
		kids := childPids(self)

		if len(kids) == 0 {
			reapZombies()

			return
		}

		for _, pid := range kids {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}

		reapZombies()
		time.Sleep(20 * time.Millisecond)
	}
}

// reapZombies reaps every already-dead child without blocking, so killed processes
// leave the table (and their own children reparent to us for the next sweep round).
func reapZombies() {
	for {
		var ws syscall.WaitStatus

		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)

		if pid <= 0 || err != nil {
			return
		}
	}
}

// childPids returns the pids of every process whose parent is `parent`, read straight
// from /proc. Best-effort — a process that exits mid-scan just drops out.
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

// ppidOf reads the parent pid out of /proc/<pid>/stat. The comm field (field 2) is
// wrapped in parens and may itself contain spaces and ')', so the fields after it are
// located from the LAST ')': state, then ppid. -1 if unreadable.
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

package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// Built-in `overseer jail` — a Go port of jail/jail.c with the same CLI:
//   overseer jail [--rw PATH | --rw=PATH]... -- CMD [ARGS...]
//
// Go's runtime is multi-threaded almost from main() onward, and per-thread
// syscalls like unshare(CLONE_NEWUSER) only affect the calling thread —
// useless when there's already a sysmon thread alongside. The trick: we use
// CLONE_NEWUSER|CLONE_NEWNS via SysProcAttr.Cloneflags, which the kernel
// applies atomically at clone(2) time, so the child is born in fresh
// namespaces with a single thread before the Go runtime spins back up.
//
// Two re-execs match jail.c's structure exactly:
//   top    → parses --rw/cmd, captures host uid/gid, spawns stage1 with
//            CLONE_NEWUSER|CLONE_NEWNS, mapping {0 → host_uid, 0 → host_gid}
//            (root inside the new user ns so we have CAP_SYS_ADMIN over it
//            for the mount work).
//   stage1 → MS_PRIVATE the mount tree, bind each --rw to itself so the
//            global RO walk leaves them alone, then remount-RO every other
//            mountpoint not in the skip-fstype list. Spawns stage2 with
//            CLONE_NEWUSER nested inside ns1, mapping {host_uid → 0} so
//            the harness sees its own uid rather than root.
//   stage2 → identity Setresuid/Setresgid (defensive — mapping already
//            puts us there) and syscall.Exec the user command. No goroutines,
//            no exec.Command — execve(2) replaces the process so the harness
//            inherits PID and stdio cleanly without an extra Go wrapper PID.
//
// Stage discriminator is a hidden flag `--__stage=mount|drop` (env vars
// leak to children; argv is explicit and stripped on the way down).
// Host uid/gid are likewise carried in argv (`--__host-uid=N`,
// `--__host-gid=N`) so stage1 doesn't have to re-parse /proc/self/uid_map.

const (
	jailStageMount = "mount"
	jailStageDrop  = "drop"
)

func jailMain(args []string) {
	stage, hostUID, hostGID, rest := extractJailInternalFlags(args)

	switch stage {
	case "":
		jailTop(rest)
	case jailStageMount:
		jailStageMountMain(rest, hostUID, hostGID)
	case jailStageDrop:
		jailStageDropMain(rest, hostUID, hostGID)
	default:
		ThrowFmt("jail: unknown --__stage=%q", stage)
	}
}

// extractJailInternalFlags strips the private --__stage / --__host-uid /
// --__host-gid flags out of the arg list (returning the cleaned tail) and
// returns their parsed values. Used by every stage; the cleaned tail is the
// normal `[--rw=...] -- cmd...` shape jail.c speaks.
func extractJailInternalFlags(args []string) (stage string, hostUID, hostGID int, rest []string) {
	hostUID = -1
	hostGID = -1
	rest = make([]string, 0, len(args))

	for _, a := range args {
		switch {
		case strings.HasPrefix(a, "--__stage="):
			stage = strings.TrimPrefix(a, "--__stage=")
		case strings.HasPrefix(a, "--__host-uid="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--__host-uid="))

			if err != nil {
				ThrowFmt("jail: bad --__host-uid: %v", err)
			}

			hostUID = n
		case strings.HasPrefix(a, "--__host-gid="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--__host-gid="))

			if err != nil {
				ThrowFmt("jail: bad --__host-gid: %v", err)
			}

			hostGID = n
		default:
			rest = append(rest, a)
		}
	}

	return
}

// jailTop is the public entry point — the one wrapJail aims at. Validates
// --rw paths up front (matching jail.c's stat+S_ISDIR/S_ISREG check) and
// re-execs the same overseer binary with CLONE_NEWUSER|CLONE_NEWNS to enter
// the namespaces atomically. Propagates the child's exit status so the
// outer caller sees the harness's real rc.
func jailTop(args []string) {
	rwPaths, cmd := parseRwAndCmd(args)

	for _, p := range rwPaths {
		st, err := os.Stat(p)

		if err != nil {
			ThrowFmt("jail: --rw %s: %v", p, err)
		}

		mode := st.Mode()

		if !mode.IsDir() && !mode.IsRegular() {
			ThrowFmt("jail: --rw %s: not a directory or regular file", p)
		}
	}

	self, err := os.Executable()

	if err != nil {
		ThrowFmt("jail: os.Executable: %v", err)
	}

	hostUID := os.Getuid()
	hostGID := os.Getgid()

	childArgs := []string{
		"jail",
		"--__stage=" + jailStageMount,
		"--__host-uid=" + strconv.Itoa(hostUID),
		"--__host-gid=" + strconv.Itoa(hostGID),
	}

	for _, p := range rwPaths {
		childArgs = append(childArgs, "--rw="+p)
	}

	childArgs = append(childArgs, "--")
	childArgs = append(childArgs, cmd...)

	proc := exec.Command(self, childArgs...)
	proc.Stdin = os.Stdin
	proc.Stdout = os.Stdout
	proc.Stderr = os.Stderr
	proc.Env = os.Environ()
	proc.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS,
		UidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: hostUID, Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: hostGID, Size: 1},
		},
		GidMappingsEnableSetgroups: false,
		Pdeathsig:                  syscall.SIGKILL,
	}

	err = proc.Run()

	if err == nil {
		return
	}

	var ee *exec.ExitError

	if errors.As(err, &ee) {
		os.Exit(ee.ExitCode())
	}

	ThrowFmt("jail: spawn stage1: %v", err)
}

// jailStageMountMain runs inside the new user+mount namespace as inner-root.
// Makes the propagation private (so our remount-RO doesn't leak back), binds
// each --rw onto itself (creates a fresh peer the RO walk will skip), then
// remounts every other mountpoint read-only. Final step: spawn stage2 in a
// nested user ns that maps inner-root back down to the real host uid, so the
// harness can't accidentally pass `if (getuid() == 0)` checks.
func jailStageMountMain(args []string, hostUID, hostGID int) {
	if hostUID < 0 || hostGID < 0 {
		ThrowFmt("jail stage1: missing --__host-uid/--__host-gid")
	}

	rwPaths, cmd := parseRwAndCmd(args)

	err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, "")

	if err != nil {
		ThrowFmt("jail stage1: mount / MS_REC|MS_PRIVATE: %v", err)
	}

	for _, p := range rwPaths {
		err := syscall.Mount(p, p, "", syscall.MS_BIND|syscall.MS_REC, "")

		if err != nil {
			ThrowFmt("jail stage1: bind %s: %v", p, err)
		}
	}

	walkMountinfoRemountRO(rwPaths)

	self, err := os.Executable()

	if err != nil {
		ThrowFmt("jail stage1: os.Executable: %v", err)
	}

	childArgs := []string{
		"jail",
		"--__stage=" + jailStageDrop,
		"--__host-uid=" + strconv.Itoa(hostUID),
		"--__host-gid=" + strconv.Itoa(hostGID),
		"--",
	}
	childArgs = append(childArgs, cmd...)

	proc := exec.Command(self, childArgs...)
	proc.Stdin = os.Stdin
	proc.Stdout = os.Stdout
	proc.Stderr = os.Stderr
	proc.Env = os.Environ()
	proc.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{
			{ContainerID: hostUID, HostID: 0, Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{ContainerID: hostGID, HostID: 0, Size: 1},
		},
		GidMappingsEnableSetgroups: false,
		Pdeathsig:                  syscall.SIGKILL,
	}

	err = proc.Run()

	if err == nil {
		return
	}

	var ee *exec.ExitError

	if errors.As(err, &ee) {
		os.Exit(ee.ExitCode())
	}

	ThrowFmt("jail stage1: spawn stage2: %v", err)
}

// jailStageDropMain runs inside the nested user ns where uid/gid already
// look like the host's. Calls Setresuid/Setresgid defensively (identity in
// practice — the mapping already gave us host_uid) and execve's the user
// command. No goroutines fire before Exec, so we stay in a clean single-
// threaded state; the harness inherits this stage's PID directly.
func jailStageDropMain(args []string, hostUID, hostGID int) {
	if hostUID < 0 || hostGID < 0 {
		ThrowFmt("jail stage2: missing --__host-uid/--__host-gid")
	}

	_, cmd := parseRwAndCmd(args)

	if len(cmd) == 0 {
		ThrowFmt("jail stage2: empty command after --")
	}

	err := syscall.Setresgid(hostGID, hostGID, hostGID)

	if err != nil {
		ThrowFmt("jail stage2: setresgid: %v", err)
	}

	err = syscall.Setresuid(hostUID, hostUID, hostUID)

	if err != nil {
		ThrowFmt("jail stage2: setresuid: %v", err)
	}

	bin, err := exec.LookPath(cmd[0])

	if err != nil {
		ThrowFmt("jail stage2: lookup %s: %v", cmd[0], err)
	}

	err = syscall.Exec(bin, cmd, os.Environ())
	ThrowFmt("jail stage2: execve %s: %v", bin, err)
}

// parseRwAndCmd splits the post-internal-flag arg list into rw paths (each
// --rw / --rw= form) and the trailing command after `--`. Mirrors jail.c's
// hand-rolled parser — flag.NewFlagSet would refuse repeated --rw and is
// overkill here.
func parseRwAndCmd(args []string) (rw []string, cmd []string) {
	i := 0

	for i < len(args) {
		a := args[i]

		if a == "--" {
			cmd = args[i+1:]

			return
		}

		if a == "--rw" {
			if i+1 >= len(args) {
				ThrowFmt("jail: --rw requires a value")
			}

			rw = append(rw, args[i+1])
			i += 2

			continue
		}

		if strings.HasPrefix(a, "--rw=") {
			rw = append(rw, strings.TrimPrefix(a, "--rw="))
			i++

			continue
		}

		ThrowFmt("jail: unexpected argument %q (usage: jail [--rw PATH]... -- CMD [ARGS...])", a)
	}

	ThrowFmt("jail: missing `--` before command")

	return
}

// walkMountinfoRemountRO walks every mount currently visible in this ns and
// remounts it read-only, skipping (a) pseudo filesystems that have no business
// being RO and frequently EPERM (proc, sysfs, cgroup, ...), and (b) anything
// under a --rw path. Per-mount errors are warnings, not fatal — matches
// jail.c's "fprintf and continue" behavior, which is what makes jail usable
// on hosts where some mounts are intrinsically un-remountable.
func walkMountinfoRemountRO(rwPaths []string) {
	f, err := os.Open("/proc/self/mountinfo")

	if err != nil {
		ThrowFmt("jail stage1: open /proc/self/mountinfo: %v", err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		line := scanner.Text()

		target, fstype, ok := parseMountinfoLine(line)

		if !ok {
			continue
		}

		if isSkipFstype(fstype) {
			continue
		}

		if isUnderAny(target, rwPaths) {
			continue
		}

		err := syscall.Mount("", target, "", syscall.MS_REMOUNT|syscall.MS_BIND|syscall.MS_RDONLY, "")

		if err != nil {
			fmt.Fprintf(os.Stderr, "jail: remount %s ro: %v (continuing)\n", target, err)
		}
	}

	if err := scanner.Err(); err != nil {
		ThrowFmt("jail stage1: scan mountinfo: %v", err)
	}
}

// parseMountinfoLine pulls (mount_point, fstype) out of one /proc/self/mountinfo
// line. The format is well-defined (kernel `Documentation/filesystems/proc.rst`):
// 5 fixed fields, then 0+ optional fields, then ` - `, then fstype source super_opts.
// We split on " " once with cap 6 to grab the first 5 fields and the tail in one
// shot; the tail is then scanned for the ` - ` separator.
func parseMountinfoLine(line string) (target, fstype string, ok bool) {
	fields := strings.SplitN(line, " ", 6)

	if len(fields) < 6 {
		return "", "", false
	}

	target = unescapeMountinfo(fields[4])

	idx := strings.Index(fields[5], " - ")

	if idx < 0 {
		return "", "", false
	}

	rest := fields[5][idx+3:]
	sp := strings.IndexByte(rest, ' ')

	if sp < 0 {
		fstype = rest
	} else {
		fstype = rest[:sp]
	}

	return target, fstype, true
}

// unescapeMountinfo decodes the kernel's \NNN octal escapes for whitespace and
// special chars in mount-point paths. Only the documented escapes (\040, \011,
// \012, \134) are emitted today, but the form is `\` + 3 octal digits so we
// just decode that pattern generically.
func unescapeMountinfo(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}

	var b strings.Builder

	for i := 0; i < len(s); {
		if i+3 < len(s) && s[i] == '\\' && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			n := (int(s[i+1]-'0') << 6) | (int(s[i+2]-'0') << 3) | int(s[i+3]-'0')
			b.WriteByte(byte(n))
			i += 4

			continue
		}

		b.WriteByte(s[i])
		i++
	}

	return b.String()
}

func isOctal(c byte) bool {
	return c >= '0' && c <= '7'
}

// isSkipFstype: pseudo-filesystems we never try to remount-RO. Verbatim from
// jail.c's list — keep the two in sync if either drifts.
func isSkipFstype(fs string) bool {
	switch fs {
	case "proc", "sysfs", "cgroup", "cgroup2", "devpts", "mqueue",
		"debugfs", "tracefs", "bpf", "securityfs", "pstore",
		"fusectl", "configfs":
		return true
	}

	return false
}

func isUnderAny(path string, roots []string) bool {
	for _, r := range roots {
		if path == r {
			return true
		}

		if strings.HasPrefix(path, r) && len(path) > len(r) && path[len(r)] == '/' {
			return true
		}
	}

	return false
}

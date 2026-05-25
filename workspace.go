package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var wsCounter uint64

func newWorkspaceID() string {
	n := atomic.AddUint64(&wsCounter, 1)

	return fmt.Sprintf("ws-%s-%04d", time.Now().UTC().Format("2006-01-02-150405"), n)
}

func wsRoot(orchRoot string) string {
	return filepath.Join(orchRoot, "workspaces")
}

func wsPath(orchRoot, id string) string {
	return filepath.Join(wsRoot(orchRoot), id)
}

func NewWorkspace(orchRoot, trunk string) string {
	id := newWorkspaceID()

	if simulate {
		return id // no clone in sim — nothing reads the workspace dir
	}

	dst := wsPath(orchRoot, id)

	Throw(os.MkdirAll(wsRoot(orchRoot), 0755))

	cmd := exec.Command("git", "clone", "--local", trunk, dst)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	Throw(cmd.Run())

	branch := "ovs/" + id

	cmd = exec.Command("git", "-C", dst, "checkout", "-b", branch)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	Throw(cmd.Run())

	return id
}

func FetchBranch(trunk, srcPath, branch string) {
	if simulate {
		return
	}

	refspec := branch + ":" + branch
	cmd := exec.Command("git", "-C", trunk, "fetch", srcPath, refspec)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	Throw(cmd.Run())
}

// Trunk's HEAD and working tree are mutated by exactly one function in this codebase:
// FfMergeBranch (post-MergedVerdict fast-forward). Nothing else here may write to either.
// In particular there is no TrunkPull / git pull / git rebase: with the user's global
// `pull.rebase=true` even `pull --ff-only` becomes a rebase that, on conflict, leaves
// trunk mid-rebase with files deleted from the working tree. If the operator wants to
// integrate remote changes into trunk's master, they do it manually between runs:
//     git -C <trunk> fetch && git -C <trunk> merge --ff-only origin/master

func readGoalsHash(trunk string) string {
	data, err := os.ReadFile(filepath.Join(trunk, "GOALS.md"))

	if err != nil {
		return ""
	}

	return sha256hex(data)
}

func FfMergeBranch(trunk, branch string) (bool, string) {
	if simulate {
		return true, "[sim] ff-merge"
	}

	fmt.Fprintf(os.Stderr, "trunk: ff-merge %s into %s\n", branch, trunk)

	cmd := exec.Command("git", "-C", trunk, "merge", "--ff-only", branch)
	out, err := cmd.CombinedOutput()

	os.Stderr.Write(out)

	if err != nil {
		fmt.Fprintf(os.Stderr, "trunk: ff-merge FAILED: %v\n", err)

		return false, string(out)
	}

	return true, string(out)
}

func CurrentTrunkHash(trunk string) string {
	cmd := exec.Command("git", "-C", trunk, "rev-parse", "HEAD")
	out, err := cmd.Output()

	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

// WorkspaceCommitsAhead reports how many commits the workspace's HEAD has beyond the
// clone-time base (origin/HEAD or origin/master). 0 means the digger emitted READY
// without committing anything; -1 means we couldn't determine (don't gate on that).
func WorkspaceCommitsAhead(wsAbs string) int {
	if simulate {
		return -1 // no real clone in sim; -1 means "don't gate on commit count"
	}

	for _, base := range []string{"origin/HEAD", "origin/master"} {
		cmd := exec.Command("git", "-C", wsAbs, "rev-list", "--count", base+"..HEAD")
		out, err := cmd.Output()

		if err != nil {
			continue
		}

		n, err := strconv.Atoi(strings.TrimSpace(string(out)))

		if err != nil {
			continue
		}

		return n
	}

	return -1
}

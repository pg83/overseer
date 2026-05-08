package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	refspec := branch + ":" + branch
	cmd := exec.Command("git", "-C", trunk, "fetch", srcPath, refspec)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	Throw(cmd.Run())
}

func TrunkPull(trunk string) {
	cmd := exec.Command("git", "-C", trunk, "pull", "--ff-only")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	_ = cmd.Run()
}

func readGoalsHash(trunk string) string {
	data, err := os.ReadFile(filepath.Join(trunk, "GOALS.md"))

	if err != nil {
		return ""
	}

	return sha256hex(data)
}

func FfMergeBranch(trunk, branch string) (bool, string) {
	cmd := exec.Command("git", "-C", trunk, "merge", "--ff-only", branch)
	out, err := cmd.CombinedOutput()

	if err != nil {
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

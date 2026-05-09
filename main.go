package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

func main() {
	exc := Try(func() {
		mainBody()
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, "overseer:", e.Error())
		os.Exit(1)
	})
}

func mainBody() {
	root := flag.String("root", "", "orchestrator root (where TASKS.md, tickets/, workspaces/ live)")
	trunk := flag.String("trunk", "", "path to git working tree being modified")
	harness := flag.String("harness", "claude", "agent harness binary (basename must contain 'claude' or 'opencode')")
	model := flag.String("model", "", "default model (lowest priority)")
	thinkModel := flag.String("think-model", "", "model for overseer/replanner/tasker (overrides --model)")
	workModel := flag.String("work-model", "", "model for digger/reviewer (overrides --model)")
	taskerModel := flag.String("tasker-model", "", "model for tasker (overrides --think-model)")
	diggerModel := flag.String("digger-model", "", "model for digger (overrides --work-model)")
	reviewerModel := flag.String("reviewer-model", "", "model for reviewer (overrides --work-model)")
	mergerModel := flag.String("merger-model", "", "model for merger (overrides --model)")
	replannerModel := flag.String("replanner-model", "", "model for replanner (overrides --think-model)")
	overseerModel := flag.String("overseer-model", "", "model for overseer (overrides --think-model)")
	jailBin := flag.String("jail-bin", "", "jail binary (PATH-resolved or absolute); empty = run harness directly")
	Throw(flag.CommandLine.Parse(os.Args[1:]))

	if *root == "" {
		ThrowFmt("--root is required")
	}

	if *trunk == "" {
		ThrowFmt("--trunk is required")
	}

	Throw(os.MkdirAll(*root, 0755))

	harnessAbs, err := exec.LookPath(*harness)

	if err != nil {
		ThrowFmt("--harness %q: %v", *harness, err)
	}

	backend := detectBackend(harnessAbs)

	jailAbs := ""

	if *jailBin != "" {
		jailAbs, err = exec.LookPath(*jailBin)

		if err != nil {
			ThrowFmt("--jail-bin %q: %v", *jailBin, err)
		}
	}

	jailDescr := jailAbs

	if jailDescr == "" {
		jailDescr = "(none)"
	}

	models := map[string]string{}

	for _, kv := range []struct {
		k string
		v string
	}{
		{"default", *model},
		{"think", *thinkModel},
		{"work", *workModel},
		{string(RoleTasker), *taskerModel},
		{string(RoleDigger), *diggerModel},
		{string(RoleReviewer), *reviewerModel},
		{string(RoleMerger), *mergerModel},
		{string(RoleReplanner), *replannerModel},
		{string(RoleOverseer), *overseerModel},
	} {
		if kv.v != "" {
			models[kv.k] = kv.v
		}
	}

	modelDescr := "(harness default)"

	if len(models) > 0 {
		var parts []string

		for _, k := range []string{"default", "think", "work", string(RoleTasker), string(RoleDigger), string(RoleReviewer), string(RoleMerger), string(RoleReplanner), string(RoleOverseer)} {
			if v, ok := models[k]; ok {
				parts = append(parts, k+"="+v)
			}
		}

		modelDescr = strings.Join(parts, " ")
	}

	uiSys("🟢", "BOOT", fmt.Sprintf("root=%s trunk=%s harness=%s backend=%s models=[%s] jail=%s", *root, *trunk, harnessAbs, backend, modelDescr, jailDescr))

	o := NewOrchestrator(*root, *trunk, harnessAbs, backend, models, jailAbs)

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := <-sigs
		uiSys("🛑", "SIGNAL", "received "+s.String()+" — stopping")
		o.StopCancel()
	}()

	o.Run()

	<-o.Stopped

	uiSys("🔚", "STOP", "overseer halted")
}

func detectBackend(harnessAbs string) Backend {
	base := strings.ToLower(filepath.Base(harnessAbs))

	if strings.Contains(base, "opencode") {
		return BackendOpencode
	}

	if strings.Contains(base, "claude") {
		return BackendClaude
	}

	ThrowFmt("--harness %q: basename must contain 'claude' or 'opencode'", harnessAbs)

	return ""
}

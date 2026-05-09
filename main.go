package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
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
	root := flag.String("root", "", "orchestrator root (where tasks.jsonl, tickets/, workspaces/ live)")
	trunk := flag.String("trunk", "", "path to git working tree being modified")

	defaultHarness := flag.String("harness", "", "default harness:model spec — '<bin>' or '<bin>:<model>'. Required.")
	thinkHarness := flag.String("think-harness", "", "harness:model for tasker / replanner / overseer (overrides --harness)")
	workHarness := flag.String("work-harness", "", "harness:model for digger / reviewer (overrides --harness)")
	taskerHarness := flag.String("tasker-harness", "", "harness:model for tasker (overrides --think-harness)")
	diggerHarness := flag.String("digger-harness", "", "harness:model for digger (overrides --work-harness)")
	reviewerHarness := flag.String("reviewer-harness", "", "harness:model for reviewer (overrides --work-harness)")
	mergerHarness := flag.String("merger-harness", "", "harness:model for merger (overrides --harness)")
	replannerHarness := flag.String("replanner-harness", "", "harness:model for replanner (overrides --think-harness)")
	overseerHarness := flag.String("overseer-harness", "", "harness:model for overseer (overrides --think-harness)")

	jailBin := flag.String("jail-bin", "", "jail binary (PATH-resolved or absolute); empty = run harness directly")
	Throw(flag.CommandLine.Parse(os.Args[1:]))

	if *root == "" {
		ThrowFmt("--root is required")
	}

	if *trunk == "" {
		ThrowFmt("--trunk is required")
	}

	if *defaultHarness == "" {
		ThrowFmt("--harness is required")
	}

	Throw(os.MkdirAll(*root, 0755))

	bindings := map[string]HarnessModel{
		"default": parseHarnessSpec("--harness", *defaultHarness),
	}

	for _, kv := range []struct {
		flag string
		key  string
		val  string
	}{
		{"--think-harness", "think", *thinkHarness},
		{"--work-harness", "work", *workHarness},
		{"--tasker-harness", string(RoleTasker), *taskerHarness},
		{"--digger-harness", string(RoleDigger), *diggerHarness},
		{"--reviewer-harness", string(RoleReviewer), *reviewerHarness},
		{"--merger-harness", string(RoleMerger), *mergerHarness},
		{"--replanner-harness", string(RoleReplanner), *replannerHarness},
		{"--overseer-harness", string(RoleOverseer), *overseerHarness},
	} {
		if kv.val == "" {
			continue
		}

		bindings[kv.key] = parseHarnessSpec(kv.flag, kv.val)
	}

	jailAbs := ""

	if *jailBin != "" {
		var err error
		jailAbs, err = exec.LookPath(*jailBin)

		if err != nil {
			ThrowFmt("--jail-bin %q: %v", *jailBin, err)
		}
	}

	jailDescr := jailAbs

	if jailDescr == "" {
		jailDescr = "(none)"
	}

	uiSys("🟢", "BOOT", fmt.Sprintf("root=%s trunk=%s bindings=[%s] jail=%s",
		*root, *trunk, formatBindings(bindings), jailDescr))

	o := NewOrchestrator(*root, *trunk, bindings, jailAbs)

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

// parseHarnessSpec splits a "<bin>" or "<bin>:<model>" CLI value into a (Harness,
// model) pair. The binary is PATH-resolved; the harness implementation is selected by
// basename via SelectHarness. The flagName is included in error messages so the user
// can tell which flag was malformed.
func parseHarnessSpec(flagName, spec string) HarnessModel {
	bin, model := spec, ""

	// Last colon — paths can contain colons in theory but not on our platforms;
	// using last colon lets users pass `:modelname` after any path.
	if idx := strings.LastIndex(spec, ":"); idx >= 0 {
		bin = spec[:idx]
		model = spec[idx+1:]
	}

	if bin == "" {
		ThrowFmt("%s %q: empty binary path", flagName, spec)
	}

	abs, err := exec.LookPath(bin)

	if err != nil {
		ThrowFmt("%s %q: %v", flagName, spec, err)
	}

	return HarnessModel{Harness: SelectHarness(abs), Model: model}
}

// formatBindings renders the resolved binding table for the BOOT log line in a fixed
// order so the output is comparable across runs.
func formatBindings(b map[string]HarnessModel) string {
	order := []string{
		"default", "think", "work",
		string(RoleTasker), string(RoleDigger), string(RoleReviewer),
		string(RoleMerger), string(RoleReplanner), string(RoleOverseer),
	}

	var parts []string

	for _, k := range order {
		hm, ok := b[k]

		if !ok {
			continue
		}

		spec := hm.Harness.Bin()

		if hm.Model != "" {
			spec += ":" + hm.Model
		}

		parts = append(parts, k+"="+spec)
	}

	return strings.Join(parts, " ")
}

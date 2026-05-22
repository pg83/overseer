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
		if len(os.Args) < 2 {
			ThrowFmt("usage: overseer {run|plan|jail|tickets|cost} [args...]")
		}

		sub := os.Args[1]
		args := os.Args[2:]

		switch sub {
		case "run":
			runMain(args)
		case "plan":
			planMain(args)
		case "jail":
			jailMain(args)
		case "tickets":
			ticketsMain(args)
		case "cost":
			costMain(args)
		default:
			ThrowFmt("unknown subcommand %q (expected: run, plan, jail, tickets, cost)", sub)
		}
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, "overseer:", e.Error())
		os.Exit(1)
	})
}

func runMain(argv []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	root := fs.String("root", "", "orchestrator root (where tasks.events.jsonl, tickets/, workspaces/ live)")
	trunk := fs.String("trunk", "", "path to git working tree being modified")

	defaultHarness := fs.String("harness", "", "default harness:model spec — '<bin>' or '<bin>:<model>'. Required.")
	thinkHarness := fs.String("think-harness", "", "harness:model for tasker / replanner / overseer (overrides --harness)")
	workHarness := fs.String("work-harness", "", "harness:model for digger / reviewer (overrides --harness)")
	taskerHarness := fs.String("tasker-harness", "", "harness:model for tasker (overrides --think-harness)")
	diggerHarness := fs.String("digger-harness", "", "harness:model for digger (overrides --work-harness)")
	reviewerHarness := fs.String("reviewer-harness", "", "harness:model for reviewer (overrides --work-harness)")
	mergerHarness := fs.String("merger-harness", "", "harness:model for merger (overrides --harness)")
	replannerHarness := fs.String("replanner-harness", "", "harness:model for replanner (overrides --think-harness)")
	overseerHarness := fs.String("overseer-harness", "", "harness:model for overseer (overrides --think-harness)")
	arbiterHarness := fs.String("arbiter-harness", "", "harness:model for arbiter (overrides --think-harness)")

	overseerDirective := fs.String("overseer", "", "operator directive: force one overseer pass at boot, injected as a mandatory instruction")
	replanDirective := fs.String("replan", "", "operator directive: force one replanner pass at boot, injected as a mandatory instruction")

	jailBin := fs.String("jail-bin", "", "external jail binary (PATH-resolved). Empty = use built-in `overseer jail`.")
	noJail := fs.Bool("no-jail", false, "run harness directly with no jail wrapper (trusted env only)")

	var extraRW []string

	fs.Func("rw", "extra path to bind read-write inside the jail (repeatable; stacks on top of workspace / harness defaults / per-task TMPDIR; no effect with --no-jail)", func(v string) error {
		extraRW = append(extraRW, v)

		return nil
	})

	Throw(fs.Parse(argv))

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
		{"--arbiter-harness", string(RoleArbiter), *arbiterHarness},
	} {
		if kv.val == "" {
			continue
		}

		bindings[kv.key] = parseHarnessSpec(kv.flag, kv.val)
	}

	jail, jailDescr := resolveJail(*jailBin, *noJail)

	uiSys("🟢", "BOOT", fmt.Sprintf("root=%s trunk=%s bindings=[%s] jail=%s",
		*root, *trunk, formatBindings(bindings), jailDescr))

	o := NewOrchestrator(*root, *trunk, bindings, jail, extraRW)
	o.bootOverseer = strings.TrimSpace(*overseerDirective)
	o.bootReplan = strings.TrimSpace(*replanDirective)

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

// resolveJail returns the jail-prefix command (passed verbatim to wrapJail)
// plus a human description for the BOOT log. Three modes:
//
//	--no-jail            → nil           (insecure direct exec, trusted hosts).
//	--jail-bin X         → ["X"]         (external jail binary, PATH-resolved).
//	neither              → [self, "jail"] (built-in `overseer jail` subcommand).
func resolveJail(jailBin string, noJail bool) (jail []string, descr string) {
	if noJail {
		return nil, "(direct, --no-jail)"
	}

	if jailBin != "" {
		abs, err := exec.LookPath(jailBin)

		if err != nil {
			ThrowFmt("--jail-bin %q: %v", jailBin, err)
		}

		return []string{abs}, abs
	}

	self, err := os.Executable()

	if err != nil {
		ThrowFmt("resolve internal jail: os.Executable: %v", err)
	}

	return []string{self, "jail"}, self + " jail (built-in)"
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
		string(RoleArbiter),
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

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
			ThrowFmt("usage: overseer {run|plan|jail|subreaper} [args...]")
		}

		sub := os.Args[1]
		args := os.Args[2:]

		switch sub {
		case "run":
			runDispatch(args)
		case "plan":
			planMain(args)
		case "jail":
			jailMain(args)
		case "subreaper":
			subreaperMain(args)
		default:
			ThrowFmt("unknown subcommand %q (expected: run, plan, jail, subreaper)", sub)
		}
	})

	exc.Catch(func(e *Exception) {
		fmt.Fprintln(os.Stderr, "overseer:", e.Error())
		os.Exit(1)
	})
}

// runDispatch routes the `run` group: `run cost` / `run tickets` inspect an
// existing run's root, anything else (flags, no args) is the orchestrator itself.
func runDispatch(argv []string) {
	if len(argv) > 0 {
		switch argv[0] {
		case "cost":
			costMain(argv[1:])

			return
		case "tickets":
			ticketsMain(argv[1:])

			return
		}
	}

	runMain(argv)
}

func runMain(argv []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	root := fs.String("root", "", "orchestrator root (where tasks.events.jsonl, tickets/, workspaces/ live)")
	trunk := fs.String("trunk", "", "path to git working tree being modified")

	defaultHarness := fs.String("harness", "", "default harness:model spec — '<bin>' or '<bin>:<model>'. Required.")
	thinkHarness := fs.String("think-harness", "", "harness:model for tasker / lead / overseer (overrides --harness)")
	workHarness := fs.String("work-harness", "", "harness:model for digger / reviewer (overrides --harness)")
	taskerHarness := fs.String("tasker-harness", "", "harness:model for tasker (overrides --think-harness)")
	diggerHarness := fs.String("digger-harness", "", "harness:model for digger (overrides --work-harness)")
	reviewerHarness := fs.String("reviewer-harness", "", "harness:model for reviewer (overrides --work-harness)")
	mergerHarness := fs.String("merger-harness", "", "harness:model for merger (overrides --harness)")
	leadHarness := fs.String("lead-harness", "", "harness:model for lead (overrides --think-harness)")
	arbiterHarness := fs.String("arbiter-harness", "", "harness:model for arbiter (overrides --think-harness)")

	replanDirective := fs.String("replan", "", "operator directive: force one lead pass at boot, injected as a mandatory instruction")

	uiMode := fs.String("ui", "log", "ui mode: log (scrolling lines) | tui (interactive tcell tabs)")

	jailBin := fs.String("jail-bin", "", "external jail binary (PATH-resolved). Empty = use built-in `overseer jail`.")
	noJail := fs.Bool("no-jail", false, "run harness directly with no jail wrapper (trusted env only)")
	noSubreaper := fs.Bool("no-subreaper", false, "do not wrap agents in `overseer subreaper` (the reaping mini-init that kills leaked agent subprocesses); independent of --no-jail")
	fs.Var(simSpec{}, "sim", "simulator: synthesize agent verdicts instead of running real harnesses (no tokens, no real workspaces, no trunk writes). Bare --sim or --sim=0 runs open-ended; --sim=N caps the project at N tickets so it winds down and exercises stop conditions")

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

	if *defaultHarness == "" && !simulate {
		ThrowFmt("--harness is required")
	}

	Throw(os.MkdirAll(*root, 0755))

	bindings := map[string]HarnessModel{}

	if *defaultHarness != "" {
		bindings["default"] = parseHarnessSpec("--harness", *defaultHarness)
	} else {
		// sim with no --harness: a placeholder binding for agentSelfBlock, never executed
		bindings["default"] = HarnessModel{Harness: SelectHarness("claude"), Model: "(sim)"}
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
		{"--lead-harness", string(RoleLead), *leadHarness},
		{"--arbiter-harness", string(RoleArbiter), *arbiterHarness},
	} {
		if kv.val == "" {
			continue
		}

		bindings[kv.key] = parseHarnessSpec(kv.flag, kv.val)
	}

	jail, jailDescr := resolveJail(*jailBin, *noJail)

	uiSys("🟢", "BOOT", fmt.Sprintf("root=%s trunk=%s bindings=[%s] jail=%s subreaper=%t",
		*root, *trunk, formatBindings(bindings), jailDescr, !*noSubreaper))

	if simulate {
		scope := "open-ended"

		if simMaxTickets > 0 {
			scope = fmt.Sprintf("cap %d tickets", simMaxTickets)
		}

		uiSys("🧪", "SIMULATION", "synthesizing agent verdicts ("+scope+") — no real harnesses, workspaces, or trunk writes")
	}

	o := NewOrchestrator(*root, *trunk, bindings, jail, extraRW)
	o.bootReplan = strings.TrimSpace(*replanDirective)
	o.Subreaper = !*noSubreaper

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := <-sigs
		uiSys("🛑", "SIGNAL", "received "+s.String()+" — stopping")
		o.StopCancel()

		// Agents each run in their own process group (Setpgid), so a terminal Ctrl-C
		// doesn't reach them through ours — group-kill the in-flight ones explicitly so
		// they don't keep running (and billing) past shutdown.
		killAllAgents()
	}()

	if strings.EqualFold(*uiMode, "tui") {
		runWithTUI(o)
	} else {
		o.Run()
		<-o.Stopped
	}

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
		string(RoleMerger), string(RoleLead), string(RoleArbiter),
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

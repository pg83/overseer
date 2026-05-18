package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

const pupaSystemPrompt = `You are PUPA, the solver in a two-agent dialog. Your partner is LUPA, the critic.

Your job: receive a question and produce a concrete solution / plan / answer. After a critique from LUPA, refine.

Protocol:
- If you have a concrete proposal this turn, end your reply with exactly one JSON line: {"plan_num": N}, where N is an integer you pick to label this version of the plan. Bump it on each new proposal.
- If you're not ready to propose (asking LUPA for clarification, partial thinking) — just write text, no marker.
- Everything before the marker is the proposal. Be specific: cite files / commands / steps. Show work.
- LUPA will either accept ({"accept_plan": N}) or send back critique. On critique, revise — emit a fresh plan with a new plan_num.

You and LUPA share the workspace at the current working directory — grep, read files, run commands as needed.`

const lupaSystemPrompt = `You are LUPA, the critic in a two-agent dialog. Your partner is PUPA, the solver.

Your job: review PUPA's proposed plan. Find what's wrong. Be harsh. Default mindset: "this plan is broken until I prove otherwise". Read sources, run code, check edges, find gaps.

Protocol:
- If the plan is solid AND you've actually verified concretely (not "looks good") — accept with exactly one JSON line: {"accept_plan": N}, where N matches PUPA's plan_num. Place it as the last line.
- Otherwise, write critique — specific weaknesses, missing edges, unclear scope. PUPA will revise. Don't write the accept marker.

You and PUPA share the workspace at the current working directory.`

type planAgent struct {
	name      string
	binding   HarnessModel
	sessionID string
	jailAbs   string
	cwd       string
}

func planMain(args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)

	pupaSpec := fs.String("pupa-harness", "", "harness:model for PUPA (solver). Required.")
	lupaSpec := fs.String("lupa-harness", "", "harness:model for LUPA (critic). Required.")
	jailBin := fs.String("jail-bin", "", "jail binary (PATH-resolved or absolute); empty = run harness directly")
	outPath := fs.String("out", "", "optional path: write the accepted final PUPA text (no marker line) here")
	maxRounds := fs.Int("max-rounds", 0, "stop after N rounds (one round = PUPA turn + LUPA turn); 0 = no cap")

	Throw(fs.Parse(args))

	if *pupaSpec == "" {
		ThrowFmt("plan: --pupa-harness is required")
	}

	if *lupaSpec == "" {
		ThrowFmt("plan: --lupa-harness is required")
	}

	pupaBinding := parseHarnessSpec("--pupa-harness", *pupaSpec)
	lupaBinding := parseHarnessSpec("--lupa-harness", *lupaSpec)

	if !pupaBinding.Harness.SupportsSession() {
		ThrowFmt("plan: harness %q has no session support (required for PUPA)", pupaBinding.Harness.Name())
	}

	if !lupaBinding.Harness.SupportsSession() {
		ThrowFmt("plan: harness %q has no session support (required for LUPA)", lupaBinding.Harness.Name())
	}

	jailAbs := ""

	if *jailBin != "" {
		abs, err := exec.LookPath(*jailBin)

		if err != nil {
			ThrowFmt("--jail-bin %q: %v", *jailBin, err)
		}

		jailAbs = abs
	}

	qBytes := Throw2(io.ReadAll(os.Stdin))
	question := strings.TrimSpace(string(qBytes))

	if question == "" {
		ThrowFmt("plan: empty stdin — pipe the question in")
	}

	cwd := Throw2(os.Getwd())

	fmt.Fprintf(os.Stderr, "🟢 plan: pupa=%s lupa=%s cwd=%s max_rounds=%d\n",
		planBindingDescr(pupaBinding), planBindingDescr(lupaBinding), cwd, *maxRounds)

	pupa := &planAgent{name: "PUPA", binding: pupaBinding, jailAbs: jailAbs, cwd: cwd}
	lupa := &planAgent{name: "LUPA", binding: lupaBinding, jailAbs: jailAbs, cwd: cwd}

	pupaInput := pupaSystemPrompt + "\n\nQUESTION:\n" + question
	lupaFirst := true

	pupaPlans := map[int]string{}
	lastPupaText := ""
	lastPlanNum := 0

	rounds := 0

	for {
		rounds++

		if *maxRounds > 0 && rounds > *maxRounds {
			ThrowFmt("plan: --max-rounds %d exhausted without accept", *maxRounds)
		}

		pupaText := pupa.turn(pupaInput)
		printTurn("PUPA", rounds, pupaText)

		if n := extractMarker(pupaText, "plan_num"); n > 0 {
			pupaPlans[n] = pupaText
			lastPupaText = pupaText
			lastPlanNum = n
		} else if lastPupaText == "" {
			lastPupaText = pupaText
		}

		var lupaInput string

		if lupaFirst {
			lupaInput = lupaSystemPrompt + "\n\nORIGINAL QUESTION:\n" + question + "\n\nPUPA's reply:\n" + pupaText
			lupaFirst = false
		} else {
			lupaInput = pupaText
		}

		lupaText := lupa.turn(lupaInput)
		printTurn("LUPA", rounds, lupaText)

		if n := extractMarker(lupaText, "accept_plan"); n > 0 {
			accepted, ok := pupaPlans[n]

			if !ok {
				fmt.Fprintf(os.Stderr, "\n⚠️  plan: LUPA accepted plan_num=%d but PUPA never emitted that N; falling back to last PUPA text (plan_num=%d)\n", n, lastPlanNum)
				accepted = lastPupaText
			}

			fmt.Fprintf(os.Stderr, "\n🎯 plan: accepted plan_num=%d after %d rounds\n", n, rounds)

			if *outPath != "" {
				body := stripMarker(accepted, "plan_num")
				Throw(os.WriteFile(*outPath, []byte(body+"\n"), 0644))
				fmt.Fprintf(os.Stderr, "📝 plan: wrote final PUPA text to %s\n", *outPath)
			}

			return
		}

		pupaInput = lupaText
	}
}

func (a *planAgent) turn(prompt string) string {
	harness := a.binding.Harness

	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	for attempt := 1; ; attempt++ {
		text, fault := a.turnOnce(prompt)

		if fault == nil {
			return text
		}

		retryable, why := harness.ClassifyFault(fault)

		if !retryable {
			ThrowFmt("plan: %s [%s] failed non-retryably (attempt %d): %s", a.name, harness.Name(), attempt, why)
		}

		fmt.Fprintf(os.Stderr, "\n⏳ %s: transient (%s), retrying in %s (attempt %d)\n", a.name, why, backoff, attempt)
		time.Sleep(backoff)

		if backoff < maxBackoff {
			backoff *= 2

			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (a *planAgent) turnOnce(prompt string) (string, *agentFault) {
	harness := a.binding.Harness
	model := a.binding.Model

	args := harness.SessionArgs(model, a.cwd, a.sessionID)

	rwArgs := []string{"--rw=" + a.cwd}

	if home := os.Getenv("HOME"); home != "" {
		for _, p := range harness.JailRWPaths(home) {
			rwArgs = append(rwArgs, "--rw="+p)
		}
	}

	bin, fullArgs := wrapJail(a.jailAbs, rwArgs, harness.Bin(), args)

	cmd := exec.Command(bin, fullArgs...)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Dir = a.cwd
	cmd.Env = os.Environ()

	stdoutPipe := Throw2(cmd.StdoutPipe())

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	Throw(cmd.Start())

	var finalText strings.Builder
	var streamFault streamErr

	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)

	for scanner.Scan() {
		line := scanner.Bytes()

		var ev map[string]any

		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}

		if a.sessionID == "" {
			if sid := harness.ParseSessionID(ev); sid != "" {
				a.sessionID = sid
			}
		}

		harness.ParseStreamLine(ev, &finalText, &streamFault, AgentRole(""), 0)
	}

	err := cmd.Wait()

	if err != nil {
		fault := &agentFault{stderr: stderrBuf.String(), stdout: finalText.String(), exitCode: -1}

		var ee *exec.ExitError

		if errors.As(err, &ee) {
			fault.exitCode = ee.ExitCode()
		} else {
			fault.stderr = fmt.Sprintf("%s\nwait error: %v", fault.stderr, err)
		}

		return finalText.String(), fault
	}

	if streamFault.set {
		return finalText.String(), &agentFault{
			stderr: "stream error: " + streamFault.msg,
			stdout: finalText.String(),
		}
	}

	return finalText.String(), nil
}

func planBindingDescr(hm HarnessModel) string {
	s := hm.Harness.Name()

	if hm.Model != "" {
		s += ":" + hm.Model
	}

	return s
}

func printTurn(name string, n int, text string) {
	fmt.Fprintf(os.Stderr, "\n============================ %s #%d ============================\n%s\n",
		name, n, strings.TrimRight(text, "\n"))
}

// extractMarker scans `text` line by line for a JSON object literal containing
// the given key with an integer value. Returns the int, or 0 if not found.
// Tolerates leading whitespace; ignores non-JSON lines and JSON without `key`.
func extractMarker(text, key string) int {
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)

		if !strings.HasPrefix(t, "{") {
			continue
		}

		var ev map[string]any

		if err := json.Unmarshal([]byte(t), &ev); err != nil {
			continue
		}

		if v, ok := ev[key]; ok {
			switch x := v.(type) {
			case float64:
				return int(x)
			case int:
				return x
			}
		}
	}

	return 0
}

// stripMarker removes single-line JSON-object literals that contain the given
// key. Returns the cleaned text trimmed of surrounding whitespace. Used for
// `--out` so the file contains the prose plan without trailing protocol noise.
func stripMarker(text, key string) string {
	var kept []string

	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)

		drop := false

		if strings.HasPrefix(t, "{") {
			var ev map[string]any

			if err := json.Unmarshal([]byte(t), &ev); err == nil {
				if _, ok := ev[key]; ok {
					drop = true
				}
			}
		}

		if !drop {
			kept = append(kept, line)
		}
	}

	return strings.TrimSpace(strings.Join(kept, "\n"))
}

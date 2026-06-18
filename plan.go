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

// stderrTee is os.Stderr with a passive copy into a buffer — the live tee
// surfaces wirez/jail/harness diagnostics in real time (a silent stderrBuf
// makes auth failures and tunnel-down errors look like a hang), and the
// captured copy is what Harness.ClassifyFault sees on fault.
type stderrTee struct {
	buf *bytes.Buffer
}

func (t *stderrTee) Write(p []byte) (int, error) {
	t.buf.Write(p)

	return os.Stderr.Write(p)
}

type planAgent struct {
	name      string
	role      AgentRole
	binding   HarnessModel
	sessionID string
	jail      []string
	extraRW   []string
	cwd       string
	subreaper bool
}

func planMain(args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)

	pupaSpec := fs.String("pupa-harness", "", "harness:model for PUPA (solver). Required.")
	lupaSpec := fs.String("lupa-harness", "", "harness:model for LUPA (critic). Required.")
	jailBin := fs.String("jail-bin", "", "external jail binary (PATH-resolved). Empty = use built-in `overseer jail`.")
	noJail := fs.Bool("no-jail", false, "run harness directly with no jail wrapper (trusted env only)")
	noSubreaper := fs.Bool("no-subreaper", false, "do not wrap agents in `overseer subreaper` (the reaping mini-init that kills leaked agent subprocesses); independent of --no-jail")
	outPath := fs.String("out", "", "optional path: write the accepted final PUPA result (no marker line) here")
	maxRounds := fs.Int("max-rounds", 0, "stop after N rounds (one round = PUPA turn + LUPA turn); 0 = no cap")

	var extraRW []string

	fs.Func("rw", "extra path to bind read-write inside the jail (repeatable; stacks on top of cwd / harness defaults / $TMPDIR; no effect with --no-jail)", func(v string) error {
		extraRW = append(extraRW, v)

		return nil
	})

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

	jail, jailDescr := resolveJail(*jailBin, *noJail)

	qBytes := Throw2(io.ReadAll(os.Stdin))
	question := strings.TrimSpace(string(qBytes))

	if question == "" {
		ThrowFmt("plan: empty stdin — pipe the question in")
	}

	cwd := Throw2(os.Getwd())

	fmt.Fprintf(os.Stderr, "🟢 plan: pupa=%s lupa=%s cwd=%s jail=%s max_rounds=%d\n",
		planBindingDescr(pupaBinding), planBindingDescr(lupaBinding), cwd, jailDescr, *maxRounds)

	subreaper := !*noSubreaper

	pupa := &planAgent{name: "PUPA", role: RolePupa, binding: pupaBinding, jail: jail, extraRW: extraRW, cwd: cwd, subreaper: subreaper}
	lupa := &planAgent{name: "LUPA", role: RoleLupa, binding: lupaBinding, jail: jail, extraRW: extraRW, cwd: cwd, subreaper: subreaper}

	pupaPrompt := strings.TrimRight(withRepoOverride(cwd, RolePupa, loadEmbedded("prompts/pupa.txt")), "\n")
	lupaPrompt := strings.TrimRight(withRepoOverride(cwd, RoleLupa, loadEmbedded("prompts/lupa.txt")), "\n")

	pupaInput := pupaPrompt + "\n\nQUESTION:\n" + question
	lupaFirst := true

	pupaResults := map[int]string{}
	lastPupaText := ""
	lastResultNum := 0

	rounds := 0

	for {
		rounds++

		if *maxRounds > 0 && rounds > *maxRounds {
			ThrowFmt("plan: --max-rounds %d exhausted without accept", *maxRounds)
		}

		printTurnHeader("PUPA", rounds, pupa.binding)
		pupaText, pupaStreamed := pupa.turn(pupaInput)
		printTurnFooter(pupaText, pupaStreamed)

		if n := extractMarker(pupaText, "result_num"); n > 0 {
			pupaResults[n] = pupaText
			lastPupaText = pupaText
			lastResultNum = n
		} else if lastPupaText == "" {
			lastPupaText = pupaText
		}

		var lupaInput string

		if lupaFirst {
			lupaInput = lupaPrompt + "\n\nORIGINAL QUESTION:\n" + question + "\n\nPUPA's reply:\n" + pupaText
			lupaFirst = false
		} else {
			lupaInput = pupaText
		}

		printTurnHeader("LUPA", rounds, lupa.binding)
		lupaText, lupaStreamed := lupa.turn(lupaInput)
		printTurnFooter(lupaText, lupaStreamed)

		if n := extractMarker(lupaText, "accept_result"); n > 0 {
			accepted, ok := pupaResults[n]

			if !ok {
				fmt.Fprintf(os.Stderr, "\n⚠️  plan: LUPA accepted result_num=%d but PUPA never emitted that N; falling back to last PUPA text (result_num=%d)\n", n, lastResultNum)
				accepted = lastPupaText
			}

			fmt.Fprintf(os.Stderr, "\n🎯 plan: accepted result_num=%d after %d rounds\n", n, rounds)

			if *outPath != "" {
				body := stripMarker(accepted, "result_num")
				Throw(os.WriteFile(*outPath, []byte(body+"\n"), 0644))
				fmt.Fprintf(os.Stderr, "📝 plan: wrote final PUPA result to %s\n", *outPath)
			}

			return
		}

		pupaInput = lupaText
	}
}

// turn drives the harness until it produces output without a retryable fault.
// Returns the assistant's finalText plus a flag indicating whether anything
// was already streamed live to stderr via LiveTextChunk (if true, the caller
// must NOT re-print finalText — that would duplicate the prose).
func (a *planAgent) turn(prompt string) (string, bool) {
	harness := a.binding.Harness

	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	for attempt := 1; ; attempt++ {
		text, streamed, fault := a.turnOnce(prompt)

		if fault == nil {
			return text, streamed
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

func (a *planAgent) turnOnce(prompt string) (string, bool, *agentFault) {
	harness := a.binding.Harness
	model := a.binding.Model

	args := harness.SessionArgs(model, a.cwd, a.sessionID)

	rwArgs := []string{"--rw=" + a.cwd}

	if home := os.Getenv("HOME"); home != "" {
		for _, p := range harness.JailRWPaths(home) {
			rwArgs = append(rwArgs, "--rw="+p)
		}
	}

	if tmpdir := strings.TrimSpace(os.Getenv("TMPDIR")); tmpdir != "" {
		rwArgs = append(rwArgs, "--rw="+tmpdir)
	}

	for _, p := range a.extraRW {
		rwArgs = append(rwArgs, "--rw="+p)
	}

	bin, fullArgs := wrapJail(a.jail, rwArgs, harness.Bin(), args)
	bin, fullArgs = wrapSubreaper(a.subreaper, bin, fullArgs)

	fmt.Fprintf(os.Stderr, "🔧 %s exec: %s %s (prompt %d bytes, session=%q)\n",
		a.name, bin, strings.Join(fullArgs, " "), len(prompt), a.sessionID)

	cmd := exec.Command(bin, fullArgs...)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Dir = a.cwd
	cmd.Env = os.Environ()

	stdoutPipe := Throw2(cmd.StdoutPipe())

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrTee{buf: &stderrBuf}

	Throw(cmd.Start())

	var finalText strings.Builder
	var streamFault streamErr
	streamed := false

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

		if chunk := harness.LiveTextChunk(ev); chunk != "" {
			fmt.Fprint(os.Stderr, chunk)
			streamed = true
		}

		harness.ParseStreamLine(ev, &finalText, &streamFault, a.role, 0)
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

		return finalText.String(), streamed, fault
	}

	if streamFault.set {
		return finalText.String(), streamed, &agentFault{
			stderr: "stream error: " + streamFault.msg,
			stdout: finalText.String(),
		}
	}

	// Empty-output guard: cmd.Wait was happy but neither finalText nor the
	// live-stream got anything. Happens when the harness writes a CLI usage
	// error to stderr and exits — depending on wrappers (subreaper / wirez /
	// jail / shell), the exit code may not surface as a Go ExitError, so
	// we'd otherwise loop forever feeding empty prompts back. Synthesize a
	// fault with whatever stderr we captured so ClassifyFault sees it.
	if strings.TrimSpace(finalText.String()) == "" && !streamed {
		return finalText.String(), streamed, &agentFault{
			stderr:   stderrBuf.String(),
			stdout:   "",
			exitCode: 0,
		}
	}

	return finalText.String(), streamed, nil
}

func planBindingDescr(hm HarnessModel) string {
	s := hm.Harness.Name()

	if hm.Model != "" {
		s += ":" + hm.Model
	}

	return s
}

// printTurnHeader is emitted BEFORE the harness launches so the user sees the
// turn boundary immediately — cold-start (jail + wirez + harness boot) can be
// many seconds before any tool trace or text arrives, and a silent wait between
// boot and the first event is hard to read.
func printTurnHeader(name string, n int, hm HarnessModel) {
	fmt.Fprintf(os.Stderr, "\n============================ %s #%d  (%s) ============================\n",
		name, n, planBindingDescr(hm))
}

// printTurnFooter closes a turn block. If `streamed` is true, the prose is
// already on screen via LiveTextChunk; print only a trailing newline as
// separator. Otherwise (harness emitted no live chunks — e.g. opencode/gemini
// or a claude turn that only produced a `result` event) dump finalText.
func printTurnFooter(text string, streamed bool) {
	if streamed {
		fmt.Fprintln(os.Stderr)

		return
	}

	fmt.Fprintf(os.Stderr, "%s\n", strings.TrimRight(text, "\n"))
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

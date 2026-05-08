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
	model := flag.String("model", "", "model name to pass to harness (opencode: -m <model>); empty = harness default")
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

	modelDescr := *model

	if modelDescr == "" {
		modelDescr = "(default)"
	}

	uiSys("🟢", "BOOT", fmt.Sprintf("root=%s trunk=%s harness=%s backend=%s model=%s jail=%s", *root, *trunk, harnessAbs, backend, modelDescr, jailDescr))

	o := NewOrchestrator(*root, *trunk, harnessAbs, backend, *model, jailAbs)

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

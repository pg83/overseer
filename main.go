package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
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
	claudeBin := flag.String("claude-bin", "claude", "claude code binary (PATH-resolved or absolute)")
	jailBin := flag.String("jail-bin", "jail", "jail binary (PATH-resolved or absolute)")
	Throw(flag.CommandLine.Parse(os.Args[1:]))

	if *root == "" {
		ThrowFmt("--root is required")
	}

	if *trunk == "" {
		ThrowFmt("--trunk is required")
	}

	Throw(os.MkdirAll(*root, 0755))

	claudeAbs, err := exec.LookPath(*claudeBin)

	if err != nil {
		ThrowFmt("--claude-bin %q: %v", *claudeBin, err)
	}

	jailAbs, err := exec.LookPath(*jailBin)

	if err != nil {
		ThrowFmt("--jail-bin %q: %v", *jailBin, err)
	}

	uiSys("🟢", "BOOT", fmt.Sprintf("root=%s trunk=%s claude=%s jail=%s", *root, *trunk, claudeAbs, jailAbs))

	o := NewOrchestrator(*root, *trunk, claudeAbs, jailAbs)

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

package main

import (
	"flag"
	"fmt"
	"os"
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

	o := NewOrchestrator(*root, *trunk, *claudeBin, *jailBin)

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := <-sigs
		fmt.Fprintln(os.Stderr, "received signal:", s, "— stopping")
		o.StopCancel()
	}()

	o.Run()

	<-o.Stopped

	fmt.Fprintln(os.Stderr, "overseer: stopped")
}

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func ticketsMain(argv []string) {
	fs := flag.NewFlagSet("run tickets", flag.ExitOnError)
	path := fs.String("path", "", "path to tasks.events.jsonl")

	Throw(fs.Parse(argv))

	if *path == "" {
		ThrowFmt("run tickets: --path is required")
	}

	abs := *path

	if !filepath.IsAbs(abs) {
		abs = Throw2(filepath.Abs(abs))
	}

	root := filepath.Dir(abs)
	tickets := LoadTasksFromPath(abs)

	renderOpenTickets(root, tickets, os.Stdout)
}

func LoadTasksFromPath(path string) []Ticket {
	f, err := os.Open(path)

	if err != nil {
		ThrowFmt("load tasks from %s: %v", path, err)
	}

	defer f.Close()

	var tickets []Ticket

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)

	for scanner.Scan() {
		line := scanner.Bytes()

		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var ev LogEvent
		Throw(json.Unmarshal(line, &ev))

		tickets = applyLogEvent(tickets, ev)
	}

	Throw(scanner.Err())

	return tickets
}

func renderOpenTickets(root string, tickets []Ticket, w io.Writer) {
	var open []Ticket

	for _, t := range tickets {
		if !t.Phase.Terminal() {
			open = append(open, t)
		}
	}

	sort.Slice(open, func(i, j int) bool {
		return open[i].N < open[j].N
	})

	if len(open) == 0 {
		fmt.Fprintln(w, "(no open tickets)")

		return
	}

	for i, t := range open {
		renderTicketDump(root, t, w)

		if i != len(open)-1 {
			fmt.Fprintln(w)
		}
	}
}

func renderTicketDump(root string, t Ticket, w io.Writer) {
	fmt.Fprintf(w, "T-%d\n", t.N)
	fmt.Fprintf(w, "  type: %s\n", ticketTypeLabel(t.Type))
	fmt.Fprintf(w, "  phase: %s\n", t.Phase)
	fmt.Fprintf(w, "  deps: %v\n", t.Deps)
	fmt.Fprintf(w, "  descr: %s\n", t.Descr)

	fmt.Fprintln(w, "  workspaces:")

	if len(t.Workspaces) == 0 {
		fmt.Fprintln(w, "    (none)")
	} else {
		for _, ws := range t.Workspaces {
			fmt.Fprintf(w, "    - %s\n", ws)
		}
	}

	planPath := ticketPlanPath(root, t.N)
	planData, err := os.ReadFile(planPath)

	fmt.Fprintln(w, "  plan:")

	if err != nil {
		fmt.Fprintln(w, "    (none)")
	} else {
		fmt.Fprintf(w, "    path: %s\n", planPath)
		writeIndentedBlock(w, "    ", strings.TrimRight(string(planData), "\n"))
	}

	fmt.Fprintln(w, "  events:")

	if len(t.Events) == 0 {
		fmt.Fprintln(w, "    (none)")
	} else {
		for _, ev := range t.Events {
			if ev.Detail == "" {
				fmt.Fprintf(w, "    - %s  %s\n", ev.Ts, ev.Kind)
			} else {
				fmt.Fprintf(w, "    - %s  %s  %s\n", ev.Ts, ev.Kind, ev.Detail)
			}
		}
	}

	fmt.Fprintln(w, "  chat:")

	msgs := strings.TrimRight(ticketMessages(root, t.N), "\n")

	if msgs == "" {
		fmt.Fprintln(w, "    (none)")
	} else {
		writeIndentedBlock(w, "    ", msgs)
	}
}

func writeIndentedBlock(w io.Writer, indent, body string) {
	if body == "" {
		return
	}

	for _, line := range strings.Split(body, "\n") {
		fmt.Fprintf(w, "%s%s\n", indent, line)
	}
}

func ticketTypeLabel(t TicketType) string {
	t = replayTicketType(t)

	if t == "" {
		return "unknown"
	}

	return string(t)
}

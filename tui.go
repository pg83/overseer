package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
)

// The TUI is an alternative front-end for `run` (--ui=tui), an immediate-mode
// tcell app: the whole screen is redrawn from the model on every input event and
// on a fast ticker. The model is the same uiEvent stream the log sink prints — so
// the `log` tab is exactly the classic output, while agents / tasks / stats are
// live views folded from those events.

const (
	tuiLogCap  = 4000 // global log ring
	tuiLaneCap = 800  // per-agent / per-task detail ring
)

// terminalKinds are the verdict / close UI kinds that mean a run for a
// (role,ticket) finished — used to flip a lane's running flag off.
var terminalKinds = map[string]bool{
	"READY": true, "CANT_DO": true, "APPROVE": true, "REWORK": true,
	"DISCARD": true, "DISCARDED": true, "MERGED": true, "FAIL": true,
	"FF_FAIL": true, "CONTINUE": true, "ESCALATE": true, "PLAN_WRITTEN": true,
	"NO_PLAN": true, "GOALS_ACHIEVED": true, "OVERSEER_DONE": true, "STALE": true,
}

// lane is one row in the agents or tasks tab: its current activity plus a ring
// buffer of its events for the detail page.
type lane struct {
	role      AgentRole
	ticket    int
	last      string
	lastEmoji string
	running   bool
	updated   time.Time
	buf       []uiEvent
}

type tuiSink struct {
	mu      sync.Mutex // guards the model fields below (written by emit on worker goroutines)
	log     []uiEvent
	agents  []*lane
	aidx    map[string]*lane
	tasks   []*lane
	tidx    map[int]*lane
	roleUSD map[AgentRole]float64
	runs    int
	start   time.Time

	// loop-only state (touched only by the render goroutine — no lock)
	scr    tcell.Screen
	tab    int // 0 log, 1 agents, 2 tasks, 3 stats
	sel    int
	scroll int
	detail *lane
}

func newTuiSink() *tuiSink {
	return &tuiSink{
		aidx:    map[string]*lane{},
		tidx:    map[int]*lane{},
		roleUSD: map[AgentRole]float64{},
		start:   time.Now(),
	}
}

func (t *tuiSink) emit(e uiEvent) {
	t.mu.Lock()

	t.log = append(t.log, e)

	if len(t.log) > tuiLogCap {
		t.log = t.log[len(t.log)-tuiLogCap:]
	}

	if e.kind == "EXEC" {
		t.runs++
	}

	if e.kind == "COST" {
		t.roleUSD[e.role] += parseUSD(e.msg)
	}

	if e.role != "" && e.ticket >= 0 {
		key := string(e.role) + "|" + strconv.Itoa(e.ticket)
		a := t.aidx[key]

		if a == nil {
			a = &lane{role: e.role, ticket: e.ticket}
			t.aidx[key] = a
			t.agents = append(t.agents, a)
		}

		if e.kind == "EXEC" {
			a.buf = nil // a fresh run starts its own detail log
		}

		laneAppend(a, e)
		a.running = !terminalKinds[e.kind]
	}

	if e.ticket >= 0 {
		k := t.tidx[e.ticket]

		if k == nil {
			k = &lane{ticket: e.ticket}
			t.tidx[e.ticket] = k
			t.tasks = append(t.tasks, k)
		}

		laneAppend(k, e)
		k.role = e.role
		k.running = !terminalKinds[e.kind]
	}

	t.mu.Unlock()
}

func laneAppend(l *lane, e uiEvent) {
	l.buf = append(l.buf, e)

	if len(l.buf) > tuiLaneCap {
		l.buf = l.buf[len(l.buf)-tuiLaneCap:]
	}

	l.updated = e.ts
	l.lastEmoji = e.emoji
	l.last = e.msg

	if l.last == "" {
		l.last = e.kind
	}
}

func parseUSD(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimPrefix(strings.TrimSpace(s), "$"), 64)

	return v
}

// runWithTUI runs the orchestrator under the TUI: orchestrator on a goroutine,
// tcell render loop on the main thread. Raw stdout/stderr (git, fatal, ...) are
// redirected to <root>/overseer.log so they can't corrupt the screen; tcell draws
// on /dev/tty regardless. Falls back to plain log mode if tcell can't init.
func runWithTUI(o *Orchestrator) {
	t := newTuiSink()

	scr, err := tcell.NewScreen()

	if err == nil {
		err = scr.Init()
	}

	if err != nil {
		uiSys("⚠️", "TUI", "tcell init failed, staying in log mode: "+err.Error())
		o.Run()
		<-o.Stopped

		return
	}

	scr.SetStyle(tcell.StyleDefault)
	t.scr = scr

	logf, ferr := os.Create(filepath.Join(o.Root, "overseer.log"))
	oldOut, oldErr := os.Stdout, os.Stderr

	if ferr == nil {
		os.Stdout, os.Stderr = logf, logf
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			scr.Fini()
			os.Stdout, os.Stderr = oldOut, oldErr
			uiOut = logSink{}
		})
	}

	uiOut = t
	uiCleanup = cleanup

	go o.Run()
	t.loop(o.StopCtx, o.StopCancel)

	<-o.Stopped
	cleanup()
}

func (t *tuiSink) loop(ctx context.Context, stop func()) {
	evCh := make(chan tcell.Event, 128)

	go func() {
		for {
			ev := t.scr.PollEvent()

			if ev == nil {
				return
			}

			evCh <- ev
		}
	}()

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	t.render()

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-evCh:
			switch ev := ev.(type) {
			case *tcell.EventResize:
				t.scr.Sync()
			case *tcell.EventKey:
				if t.handleKey(ev, stop) {
					return
				}
			}

			t.render()
		case <-tick.C:
			t.render()
		}
	}
}

// handleKey mutates nav state; returns true to quit.
func (t *tuiSink) handleKey(ev *tcell.EventKey, stop func()) bool {
	if ev.Key() == tcell.KeyCtrlC || ev.Rune() == 'q' {
		stop()

		return true
	}

	if t.detail != nil {
		switch ev.Key() {
		case tcell.KeyEscape, tcell.KeyLeft:
			t.detail = nil
			t.scroll = 0
		case tcell.KeyUp:
			t.scroll++
		case tcell.KeyDown:
			if t.scroll > 0 {
				t.scroll--
			}
		}

		return false
	}

	switch ev.Key() {
	case tcell.KeyLeft:
		if t.tab > 0 {
			t.tab--
		}

		t.sel, t.scroll = 0, 0
	case tcell.KeyRight:
		if t.tab < 3 {
			t.tab++
		}

		t.sel, t.scroll = 0, 0
	case tcell.KeyUp:
		if t.tab == 0 {
			t.scroll++
		} else if t.sel > 0 {
			t.sel--
		}
	case tcell.KeyDown:
		if t.tab == 0 {
			if t.scroll > 0 {
				t.scroll--
			}
		} else {
			t.sel++
		}
	case tcell.KeyEnter:
		t.openDetail()
	}

	return false
}

func (t *tuiSink) openDetail() {
	t.mu.Lock()
	defer t.mu.Unlock()

	var lanes []*lane

	switch t.tab {
	case 1:
		lanes = t.sortedAgents()
	case 2:
		lanes = t.sortedTasks()
	default:
		return
	}

	if t.sel < len(lanes) {
		t.detail = lanes[t.sel]
		t.scroll = 0
	}
}

// sortedAgents / sortedTasks assume the caller holds t.mu.
func (t *tuiSink) sortedAgents() []*lane {
	out := append([]*lane{}, t.agents...)

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].running != out[j].running {
			return out[i].running
		}

		return out[i].updated.After(out[j].updated)
	})

	return out
}

func (t *tuiSink) sortedTasks() []*lane {
	out := append([]*lane{}, t.tasks...)

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ticket < out[j].ticket
	})

	return out
}

func (t *tuiSink) render() {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.scr
	s.Clear()

	w, h := s.Size()

	t.drawTabs()

	switch {
	case t.detail != nil:
		t.drawDetail(w, h)
	case t.tab == 0:
		t.drawLog(w, h)
	case t.tab == 1:
		t.drawAgents(w, h)
	case t.tab == 2:
		t.drawTasks(w, h)
	case t.tab == 3:
		t.drawStats(w, h)
	}

	t.drawHelp(w, h)
	s.Show()
}

func (t *tuiSink) drawTabs() {
	x := 0

	for i, n := range []string{"log", "agents", "tasks", "stats"} {
		st := tcell.StyleDefault.Foreground(tcell.ColorGray)

		if i == t.tab {
			st = tcell.StyleDefault.Bold(true).Reverse(true)
		}

		x = putStr(t.scr, x, 0, st, " "+n+" ")
		x++
	}
}

func (t *tuiSink) drawHelp(w, h int) {
	help := "←/→ tabs   ↑/↓ move   enter open   q quit"

	if t.detail != nil {
		help = "↑/↓ scroll   esc back   q quit"
	}

	putStr(t.scr, 0, h-1, tcell.StyleDefault.Foreground(tcell.ColorGray), help)
}

func (t *tuiSink) drawLog(w, h int) {
	t.drawEvents(t.log, w, h)
}

func (t *tuiSink) drawEvents(events []uiEvent, w, h int) {
	top, bottom := 2, h-2
	rows := bottom - top + 1

	if rows < 1 {
		return
	}

	end := len(events) - t.scroll
	end = clampInt(end, 0, len(events))
	start := clampInt(end-rows, 0, end)

	y := top

	for i := start; i < end; i++ {
		t.drawEvent(y, w, events[i])
		y++
	}
}

func (t *tuiSink) drawEvent(y, w int, e uiEvent) {
	s := t.scr
	dim := tcell.StyleDefault.Foreground(tcell.ColorGray)

	x := putStr(s, 0, y, dim, e.ts.Format("15:04:05")+" ")

	if e.cost != "" {
		x = putStr(s, x, y, tcell.StyleDefault.Foreground(tcell.ColorDarkGray), fmt.Sprintf("%7s ", e.cost))
	}

	x = putStr(s, x, y, tcell.StyleDefault, e.emoji+" ")
	x = putStr(s, x, y, tcell.StyleDefault.Bold(true), fmt.Sprintf("%-5s ", ticketLabel(e.ticket)))
	x = putStr(s, x, y, roleStyle(e.role), fmt.Sprintf("%-10s ", roleName(e.role)))
	x = putStr(s, x, y, tcell.StyleDefault.Bold(true), e.kind+" ")
	drawClip(s, x, y, w, tcell.StyleDefault, e.msg)
}

func (t *tuiSink) drawAgents(w, h int) {
	lanes := t.sortedAgents()
	t.drawLaneList(lanes, w, h, true)
}

func (t *tuiSink) drawTasks(w, h int) {
	lanes := t.sortedTasks()
	t.drawLaneList(lanes, w, h, false)
}

func (t *tuiSink) drawLaneList(lanes []*lane, w, h int, showRoleCol bool) {
	if len(lanes) > 0 {
		t.sel = clampInt(t.sel, 0, len(lanes)-1)
	}

	y := 2

	for i, l := range lanes {
		if y > h-2 {
			break
		}

		row := tcell.StyleDefault

		if i == t.sel {
			row = row.Reverse(true)
		}

		dot := "  "

		if l.running {
			dot = "● "
		}

		x := putStr(t.scr, 0, y, row.Foreground(tcell.ColorGreen), dot)
		x = putStr(t.scr, x, y, row, fmt.Sprintf("%-5s ", ticketLabel(l.ticket)))

		rs := roleStyle(l.role)

		if i == t.sel {
			rs = row
		}

		x = putStr(t.scr, x, y, rs, fmt.Sprintf("%-10s ", roleName(l.role)))
		drawClip(t.scr, x, y, w, row, l.lastEmoji+" "+l.last)

		y++
	}
}

func (t *tuiSink) drawDetail(w, h int) {
	l := t.detail
	title := fmt.Sprintf("%s  %s  —  %d events", ticketLabel(l.ticket), roleName(l.role), len(l.buf))
	putStr(t.scr, 0, 1, tcell.StyleDefault.Bold(true), title)
	t.drawEvents(l.buf, w, h)
}

func (t *tuiSink) drawStats(w, h int) {
	up := time.Since(t.start).Round(time.Second)
	session := 0.0

	for _, v := range t.roleUSD {
		session += v
	}

	lines := []string{
		fmt.Sprintf("uptime:           %s", up),
		fmt.Sprintf("runs (session):   %d", t.runs),
		fmt.Sprintf("cost (all-time):  $%.2f", meter.totalUSD()),
		fmt.Sprintf("cost (session):   $%.2f", session),
	}

	if hrs := up.Hours(); hrs > 0.01 {
		lines = append(lines, fmt.Sprintf("rate:             $%.2f/h   %.1f runs/h", session/hrs, float64(t.runs)/hrs))
	}

	type rc struct {
		r AgentRole
		v float64
	}

	var rs []rc

	for r, v := range t.roleUSD {
		rs = append(rs, rc{r, v})
	}

	sort.Slice(rs, func(i, j int) bool { return rs[i].v > rs[j].v })

	lines = append(lines, "", "cost by role (session):")

	for _, x := range rs {
		lines = append(lines, fmt.Sprintf("  %-10s  $%.2f", roleName(x.r), x.v))
	}

	active := 0

	for _, k := range t.tasks {
		if k.running {
			active++
		}
	}

	lines = append(lines, "", fmt.Sprintf("tasks seen: %d  (%d active)", len(t.tasks), active))

	y := 2

	for _, ln := range lines {
		putStr(t.scr, 2, y, tcell.StyleDefault, ln)
		y++
	}
}

func putStr(s tcell.Screen, x, y int, st tcell.Style, str string) int {
	for _, r := range str {
		s.SetContent(x, y, r, nil, st)
		x++
	}

	return x
}

func drawClip(s tcell.Screen, x, y, maxx int, st tcell.Style, str string) {
	for _, r := range str {
		if x >= maxx {
			break
		}

		s.SetContent(x, y, r, nil, st)
		x++
	}
}

func roleStyle(r AgentRole) tcell.Style {
	c := tcell.ColorSilver

	switch r {
	case RoleTasker:
		c = tcell.ColorBlue
	case RoleDigger:
		c = tcell.ColorYellow
	case RoleReviewer:
		c = tcell.ColorFuchsia
	case RoleMerger:
		c = tcell.ColorTeal
	case RoleReplanner:
		c = tcell.ColorGreen
	case RoleOverseer:
		c = tcell.ColorRed
	}

	return tcell.StyleDefault.Foreground(c)
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}

	if v > hi {
		return hi
	}

	return v
}

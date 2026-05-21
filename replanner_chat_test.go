package main

import "testing"

func TestHandleResultChatQueuesLineForReplanner(t *testing.T) {
	o := &Orchestrator{}

	o.handleResult(AgentResult{Kind: "chat", ChatLine: "2026-01-01T00:00:00Z\ttasker\tT-1\tnote\n"})

	if len(o.replanChat) != 1 || o.replanChat[0] != "2026-01-01T00:00:00Z\ttasker\tT-1\tnote" {
		t.Fatalf("replanChat = %#v", o.replanChat)
	}
}

func TestDispatchReplannerDrainsQueuedChatIntoJob(t *testing.T) {
	o := &Orchestrator{
		Tickets:    []Ticket{{N: 1, Phase: PhaseEscalate, Descr: "x", Prio: 1}},
		shadow:     map[int]Shadow{1: ShadowStopped},
		branchWS:   map[int]string{},
		arb:        map[int]arbCtx{},
		jobs:       map[AgentRole]chan Job{RoleReplanner: make(chan Job, 1)},
		replanChat: []string{"line one", "line two"},
	}

	o.dispatchReplanner()

	job := <-o.jobs[RoleReplanner]

	if len(job.ChatLog) != 2 || job.ChatLog[0] != "line one" || job.ChatLog[1] != "line two" {
		t.Fatalf("job.ChatLog = %#v", job.ChatLog)
	}

	if len(o.replanChat) != 0 {
		t.Fatalf("replanChat should be drained, got %#v", o.replanChat)
	}
}

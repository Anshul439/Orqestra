package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func makePayload(t *testing.T, command, previousOutput string) string {
	t.Helper()
	b, err := json.Marshal(struct {
		Command        string `json:"command"`
		PreviousOutput string `json:"previous_output,omitempty"`
	}{Command: command, PreviousOutput: previousOutput})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return string(b)
}

func TestExecuteJob_InjectsPreviousOutput(t *testing.T) {
	payload := makePayload(t, `printf '%s' "$PREVIOUS_OUTPUT"`, "hello-from-prev")
	success, errMsg, output := executeJob(context.Background(), payload, slog.Default())
	if !success {
		t.Fatalf("expected success, got error: %s", errMsg)
	}
	if output != "hello-from-prev" {
		t.Errorf("output = %q, want %q", output, "hello-from-prev")
	}
}

func TestExecuteJob_EmptyPreviousOutputOnFirstStep(t *testing.T) {
	// Step 0 always receives an empty previous_output; the env var must be present but empty.
	payload := makePayload(t, `printf 'got:[%s]' "$PREVIOUS_OUTPUT"`, "")
	success, errMsg, output := executeJob(context.Background(), payload, slog.Default())
	if !success {
		t.Fatalf("expected success, got error: %s", errMsg)
	}
	if output != "got:[]" {
		t.Errorf("output = %q, want %q", output, "got:[]")
	}
}

func TestExecuteJob_TruncatesOversizedPreviousOutput(t *testing.T) {
	const limit = 64 * 1024 // must match maxPreviousOutputBytes in executeJob
	big := strings.Repeat("a", limit+1)

	payload := makePayload(t, `printf '%s' "$PREVIOUS_OUTPUT"`, big)
	success, errMsg, output := executeJob(context.Background(), payload, slog.Default())
	if !success {
		t.Fatalf("expected success, got error: %s", errMsg)
	}
	if len(output) != limit {
		t.Errorf("PREVIOUS_OUTPUT length = %d, want %d (truncated to limit)", len(output), limit)
	}
}

func TestExecuteJob_CommandFailure(t *testing.T) {
	payload := makePayload(t, "exit 1", "")
	success, errMsg, _ := executeJob(context.Background(), payload, slog.Default())
	if success {
		t.Fatal("expected failure for 'exit 1'")
	}
	if errMsg == "" {
		t.Error("expected non-empty error message on failure")
	}
}

func TestExecuteJob_InvalidPayload(t *testing.T) {
	success, errMsg, _ := executeJob(context.Background(), "not-json", slog.Default())
	if success {
		t.Fatal("expected failure for invalid JSON payload")
	}
	if !strings.Contains(errMsg, "invalid payload") {
		t.Errorf("error message %q should mention 'invalid payload'", errMsg)
	}
}

func TestExecuteJob_MissingCommand(t *testing.T) {
	success, errMsg, _ := executeJob(context.Background(), `{"previous_output":"foo"}`, slog.Default())
	if success {
		t.Fatal("expected failure when command field is absent")
	}
	if !strings.Contains(errMsg, "missing required field") {
		t.Errorf("error message %q should mention 'missing required field'", errMsg)
	}
}

package workflow_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Anshul439/Orqestra/internal/workflow"
)

func TestLoadFromDir_Valid(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "ci.yaml", "name: ci\nsteps:\n  - command: echo hello\n")

	r := workflow.NewRegistry()
	if err := workflow.LoadFromDir(r, dir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wf, ok := r.Get("ci")
	if !ok {
		t.Fatal("expected workflow 'ci' to be registered")
	}
	if len(wf.Steps) != 1 || wf.Steps[0].Command != "echo hello" {
		t.Errorf("unexpected workflow steps: %+v", wf.Steps)
	}
}

func TestLoadFromDir_NonExistentDir(t *testing.T) {
	r := workflow.NewRegistry()
	if err := workflow.LoadFromDir(r, "/tmp/no-such-dir-xyz-abc"); err != nil {
		t.Fatalf("missing dir should be silently ignored, got: %v", err)
	}
	if len(r.List()) != 0 {
		t.Error("expected empty registry")
	}
}

func TestLoadFromDir_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "bad.yaml", "name: [unclosed bracket")

	r := workflow.NewRegistry()
	if err := workflow.LoadFromDir(r, dir); err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoadFromDir_SkipsNonYAML(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "readme.txt", "ignore me")

	r := workflow.NewRegistry()
	if err := workflow.LoadFromDir(r, dir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r.List()) != 0 {
		t.Errorf("expected empty registry, got %d workflows", len(r.List()))
	}
}

func TestRegistryGet_Found(t *testing.T) {
	r := workflow.NewRegistry()
	r.Register(workflow.Workflow{Name: "deploy", Steps: []workflow.Step{{Command: "make deploy"}}})

	wf, ok := r.Get("deploy")
	if !ok {
		t.Fatal("expected to find 'deploy'")
	}
	if wf.Name != "deploy" {
		t.Errorf("got name %q, want %q", wf.Name, "deploy")
	}
}

func TestRegistryGet_NotFound(t *testing.T) {
	r := workflow.NewRegistry()
	if _, ok := r.Get("nonexistent"); ok {
		t.Fatal("expected not found")
	}
}

func TestRegistryList(t *testing.T) {
	r := workflow.NewRegistry()
	r.Register(workflow.Workflow{Name: "a"})
	r.Register(workflow.Workflow{Name: "b"})

	if n := len(r.List()); n != 2 {
		t.Errorf("expected 2 workflows, got %d", n)
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

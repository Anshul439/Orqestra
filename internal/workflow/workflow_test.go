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

func TestNormalize_SequentialMode(t *testing.T) {
	wf := workflow.Workflow{
		Name: "seq",
		Steps: []workflow.Step{
			{Command: "echo a"},
			{Command: "echo b"},
			{Command: "echo c"},
		},
	}
	wf.Normalize()

	if wf.Steps[0].DependsOn != nil {
		t.Errorf("step 0 should have no deps, got %v", wf.Steps[0].DependsOn)
	}
	if len(wf.Steps[1].DependsOn) != 1 || wf.Steps[1].DependsOn[0] != "step-0" {
		t.Errorf("step 1 should depend on step-0, got %v", wf.Steps[1].DependsOn)
	}
	if len(wf.Steps[2].DependsOn) != 1 || wf.Steps[2].DependsOn[0] != "step-1" {
		t.Errorf("step 2 should depend on step-1, got %v", wf.Steps[2].DependsOn)
	}
}

func TestNormalize_DAGMode(t *testing.T) {
	wf := workflow.Workflow{
		Name: "dag",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a"},
			{ID: "b", Command: "echo b"},
			{ID: "c", Command: "echo c", DependsOn: []string{"a", "b"}},
		},
	}
	wf.Normalize()

	if wf.Steps[0].DependsOn != nil {
		t.Errorf("step a should be a root (nil deps), got %v", wf.Steps[0].DependsOn)
	}
	if wf.Steps[1].DependsOn != nil {
		t.Errorf("step b should be a root (nil deps), got %v", wf.Steps[1].DependsOn)
	}
	if len(wf.Steps[2].DependsOn) != 2 {
		t.Errorf("step c should have 2 deps, got %v", wf.Steps[2].DependsOn)
	}
}

func TestValidate_Cycle(t *testing.T) {
	wf := workflow.Workflow{
		Name: "cyclic",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a", DependsOn: []string{"b"}},
			{ID: "b", Command: "echo b", DependsOn: []string{"a"}},
		},
	}
	if err := wf.Validate(); err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestValidate_UnknownDep(t *testing.T) {
	wf := workflow.Workflow{
		Name: "bad-dep",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a", DependsOn: []string{"nonexistent"}},
		},
	}
	if err := wf.Validate(); err == nil {
		t.Fatal("expected unknown dep error, got nil")
	}
}

func TestValidate_DuplicateID(t *testing.T) {
	wf := workflow.Workflow{
		Name: "dup",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a"},
			{ID: "a", Command: "echo a again"},
		},
	}
	if err := wf.Validate(); err == nil {
		t.Fatal("expected duplicate ID error, got nil")
	}
}

func TestUnblockedSteps_RespectsDependencies(t *testing.T) {
	wf := workflow.Workflow{
		Name: "test",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a"},
			{ID: "b", Command: "echo b"},
			{ID: "c", Command: "echo c", DependsOn: []string{"a", "b"}},
		},
	}
	wf.Normalize()

	unblocked := wf.UnblockedSteps(map[string]bool{}, map[int]bool{})
	if len(unblocked) != 2 {
		t.Fatalf("expected 2 unblocked (a, b), got %d: %v", len(unblocked), unblocked)
	}

	unblocked = wf.UnblockedSteps(map[string]bool{"a": true}, map[int]bool{0: true, 1: true})
	if len(unblocked) != 0 {
		t.Errorf("expected 0 unblocked (b already submitted, c still waiting), got %v", unblocked)
	}

	unblocked = wf.UnblockedSteps(map[string]bool{"a": true, "b": true}, map[int]bool{0: true, 1: true})
	if len(unblocked) != 1 || unblocked[0] != 2 {
		t.Errorf("expected c (index 2) to be unblocked, got %v", unblocked)
	}
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

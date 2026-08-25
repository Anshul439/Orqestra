package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Step struct {
	ID        string   `yaml:"id"`
	Command   string   `yaml:"command"`
	DependsOn []string `yaml:"depends_on,omitempty"`
	Retries   int      `yaml:"retries,omitempty"`
}

type Workflow struct {
	Name     string `yaml:"name"`
	Schedule string `yaml:"schedule,omitempty"`
	Steps    []Step `yaml:"steps"`
}

// Normalize auto-assigns IDs and fills sequential deps when no step declares
// depends_on. If any step has depends_on set, nil DependsOn means root.
func (w *Workflow) Normalize() {
	for i := range w.Steps {
		if w.Steps[i].ID == "" {
			w.Steps[i].ID = fmt.Sprintf("step-%d", i)
		}
	}

	dagMode := false
	for _, s := range w.Steps {
		if s.DependsOn != nil {
			dagMode = true
			break
		}
	}

	if !dagMode {
		for i := range w.Steps {
			if i > 0 {
				w.Steps[i].DependsOn = []string{w.Steps[i-1].ID}
			}
		}
	}
}

func (w *Workflow) Validate() error {
	w.Normalize()

	seen := make(map[string]bool, len(w.Steps))
	for _, s := range w.Steps {
		if seen[s.ID] {
			return fmt.Errorf("duplicate step id %q", s.ID)
		}
		seen[s.ID] = true
	}

	for _, s := range w.Steps {
		for _, dep := range s.DependsOn {
			if !seen[dep] {
				return fmt.Errorf("step %q depends on unknown step %q", s.ID, dep)
			}
		}
	}

	return w.detectCycles()
}

func (w *Workflow) detectCycles() error {
	indexByID := make(map[string]int, len(w.Steps))
	for i, s := range w.Steps {
		indexByID[s.ID] = i
	}

	type color int
	const (
		white color = iota
		gray
		black
	)
	colors := make([]color, len(w.Steps))

	var dfs func(i int) error
	dfs = func(i int) error {
		if colors[i] == black {
			return nil
		}
		if colors[i] == gray {
			return fmt.Errorf("cycle detected involving step %q", w.Steps[i].ID)
		}
		colors[i] = gray
		for _, dep := range w.Steps[i].DependsOn {
			if err := dfs(indexByID[dep]); err != nil {
				return err
			}
		}
		colors[i] = black
		return nil
	}

	for i := range w.Steps {
		if err := dfs(i); err != nil {
			return err
		}
	}
	return nil
}

func (w *Workflow) UnblockedSteps(completedIDs map[string]bool, submitted map[int]bool) []int {
	var ready []int
	for i, s := range w.Steps {
		if submitted[i] {
			continue
		}
		allDone := true
		for _, dep := range s.DependsOn {
			if !completedIDs[dep] {
				allDone = false
				break
			}
		}
		if allDone {
			ready = append(ready, i)
		}
	}
	return ready
}

type Registry struct {
	workflows map[string]Workflow
}

func NewRegistry() *Registry {
	return &Registry{workflows: make(map[string]Workflow)}
}

func (r *Registry) Register(w Workflow) {
	r.workflows[w.Name] = w
}

func (r *Registry) Get(name string) (Workflow, bool) {
	w, ok := r.workflows[name]
	return w, ok
}

func (r *Registry) List() []Workflow {
	list := make([]Workflow, 0, len(r.workflows))
	for _, w := range r.workflows {
		list = append(list, w)
	}
	return list
}

// LoadFromDir loads *.yaml files from dir; missing dir is silently ignored.
func LoadFromDir(r *Registry, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading workflows dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return fmt.Errorf("reading %s: %w", entry.Name(), err)
		}
		var wf Workflow
		if err := yaml.Unmarshal(data, &wf); err != nil {
			return fmt.Errorf("parsing %s: %w", entry.Name(), err)
		}
		if err := wf.Validate(); err != nil {
			return fmt.Errorf("validating %s: %w", entry.Name(), err)
		}
		r.Register(wf)
	}
	return nil
}

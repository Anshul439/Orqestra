package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Step is a single shell command in a workflow.
type Step struct {
	Command string `yaml:"command"`
}

type Workflow struct {
	Name  string `yaml:"name"`
	Steps []Step `yaml:"steps"`
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
		r.Register(wf)
	}
	return nil
}

package scheduler

import (
	"context"
	"log/slog"

	"github.com/robfig/cron/v3"

	"github.com/Anshul439/Orqestra/internal/service"
	"github.com/Anshul439/Orqestra/internal/workflow"
)

// Start blocks until ctx is cancelled.
func Start(ctx context.Context, registry *workflow.Registry, workflowSvc *service.WorkflowService) {
	c := cron.New()

	for _, wf := range registry.List() {
		if wf.Schedule == "" {
			continue
		}
		name := wf.Name
		schedule := wf.Schedule
		_, err := c.AddFunc(wf.Schedule, func() {
			active, err := workflowSvc.HasActiveRun(ctx, name)
			if err != nil {
				slog.Default().Error("scheduler: failed to check active run",
					slog.String("workflow", name),
					slog.String("error", err.Error()),
				)
				return
			}
			if active {
				slog.Default().Warn("scheduler: skipping — previous run still active",
					slog.String("workflow", name),
				)
				return
			}
			if _, err := workflowSvc.TriggerWorkflow(ctx, name); err != nil {
				slog.Default().Error("scheduler: failed to trigger workflow",
					slog.String("workflow", name),
					slog.String("schedule", schedule),
					slog.String("error", err.Error()),
				)
			}
		})
		if err != nil {
			slog.Default().Error("scheduler: invalid cron expression",
				slog.String("workflow", name),
				slog.String("schedule", schedule),
				slog.String("error", err.Error()),
			)
			continue
		}
		slog.Default().Info("scheduler: registered",
			slog.String("workflow", name),
			slog.String("schedule", schedule),
		)
	}

	c.Start()
	<-ctx.Done()
	c.Stop()
}

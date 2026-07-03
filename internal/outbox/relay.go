package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pollInterval    = 2 * time.Second
	cleanupInterval = 1 * time.Hour
	cleanupMaxAge   = 24 * time.Hour
)

func Start(ctx context.Context, pool *pgxpool.Pool, q queue.Queue) {
	log := slog.Default()

	go func() {
		cleanupTicker := time.NewTicker(cleanupInterval)
		defer cleanupTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-cleanupTicker.C:
				deleted, err := db.CleanupProcessedOutbox(ctx, pool, cleanupMaxAge)
				if err != nil {
					log.Error("outbox cleanup error", slog.String("error", err.Error()))
				} else if deleted > 0 {
					log.Info("outbox cleanup complete", slog.Int64("deleted_rows", deleted))
				}
			}
		}
	}()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := relay(ctx, pool, q, log); err != nil {
				log.Error("outbox relay error", slog.String("error", err.Error()))
			}
		}
	}
}

func relay(ctx context.Context, pool *pgxpool.Pool, q queue.Queue, log *slog.Logger) error {
	entries, tx, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return tx.Commit(ctx)
	}

	for _, e := range entries {
		job := queue.Job{
			ID:         e.JobID,
			Type:       e.Type,
			Payload:    e.Payload,
			MaxRetries: e.MaxRetries,
		}
		if err := q.Enqueue(ctx, job); err != nil {
			log.Error("failed to enqueue job from outbox",
				slog.Int("job_id", e.JobID),
				slog.String("error", err.Error()),
			)
			tx.Rollback(ctx)
			return err
		}
		if err := db.MarkOutboxProcessed(ctx, tx, e.ID); err != nil {
			log.Error("failed to mark outbox entry processed",
				slog.Int("outbox_id", e.ID),
				slog.String("error", err.Error()),
			)
			tx.Rollback(ctx)
			return err
		}
	}

	return tx.Commit(ctx)
}

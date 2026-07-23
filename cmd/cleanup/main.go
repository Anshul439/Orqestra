package main

import (
	"context"
	"fmt"
	"github.com/Anshul439/Orqestra/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.LoadConfig()
	ctx := context.Background()

	// Clear Redis
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	err := rdb.FlushAll(ctx).Err()
	if err != nil {
		fmt.Printf("Redis error: %v\n", err)
	} else {
		fmt.Println("cleared redis queue")
	}

	// Clear Postgres
	pool, err := pgxpool.New(ctx, cfg.DBUrl)
	if err != nil {
		fmt.Printf("Postgres error: %v\n", err)
		return
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, "TRUNCATE jobs, job_outbox, workflow_runs CASCADE")
	if err != nil {
		fmt.Printf("Postgres Truncate error: %v\n", err)
	} else {
		fmt.Println("cleared postgres database")
	}
}

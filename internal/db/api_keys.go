package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func InsertAPIKey(ctx context.Context, pool *pgxpool.Pool, name, keyHash string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO api_keys (name, key_hash) VALUES ($1, $2)`,
		name, keyHash,
	)
	return err
}

func ValidateAPIKey(pool *pgxpool.Pool, keyHash string) (bool, error) {
	var exists bool
	err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM api_keys WHERE key_hash = $1)`,
		keyHash,
	).Scan(&exists)
	return exists, err
}

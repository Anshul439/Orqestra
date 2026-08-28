package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/Anshul439/Orqestra/internal/config"
	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	name := "default"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	cfg := config.LoadConfig()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cfg.DBUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to postgres: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate key: %v\n", err)
		os.Exit(1)
	}

	key := "orq_" + hex.EncodeToString(raw)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))

	if err := db.InsertAPIKey(ctx, pool, name, hash); err != nil {
		fmt.Fprintf(os.Stderr, "failed to store key: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("API key (save this — it will not be shown again):\n%s\n", key)
}

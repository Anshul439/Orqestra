package api

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

func authMiddleware(pool *pgxpool.Pool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		key, ok := strings.CutPrefix(authHeader, "Bearer ")
		if !ok || key == "" {
			writeError(w, http.StatusUnauthorized, "missing API key")
			return
		}

		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
		valid, err := db.ValidateAPIKey(pool, hash)
		if err != nil || !valid {
			writeError(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		next.ServeHTTP(w, r)
	})
}

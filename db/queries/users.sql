-- name: GetUserByEmail :one
SELECT id, email, password_hash, created_at, updated_at
FROM users
WHERE email = $1;

-- name: CreateUser :one
INSERT INTO users (id, email, password_hash)
VALUES (gen_random_uuid(), $1, $2)
RETURNING id, email, password_hash, created_at, updated_at;
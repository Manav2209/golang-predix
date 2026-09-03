-- name: GetUserByEmail :one
SELECT id, email, password_hash, created_at, updated_at
FROM users
WHERE email = $1;

-- name: CreateUser :one
INSERT INTO users (id, email, password_hash)
VALUES (gen_random_uuid(), $1, $2)
RETURNING id, email, password_hash, created_at, updated_at;

-- name: GetUserBalance :one
SELECT balance
FROM users
WHERE id = $1;

-- name: DeductBalance :execrows
UPDATE users
SET balance = balance - $2,
    updated_at = NOW()
WHERE id = $1
  AND balance >= $2;

-- name: AddBalance :exec
UPDATE users
SET balance = balance + $2,
    updated_at = NOW()
WHERE id = $1;

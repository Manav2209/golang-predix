-- name: UpsertPosition :exec
INSERT INTO positions (
    user_id,
    event_id,
    outcome,
    shares,
    avg_price
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (user_id, event_id, outcome)
DO UPDATE SET
    shares = positions.shares + EXCLUDED.shares,
    avg_price = EXCLUDED.avg_price,
    updated_at = NOW();

-- name: DecreasePosition :execrows
UPDATE positions
SET
    shares = shares - $4,
    updated_at = NOW()
WHERE user_id = $1
  AND event_id = $2
  AND outcome = $3
  AND shares >= $4;

-- name: GetPosition :one
SELECT *
FROM positions
WHERE user_id = $1
  AND event_id = $2
  AND outcome = $3;
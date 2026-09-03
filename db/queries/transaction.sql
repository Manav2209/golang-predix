-- name: InsertTransaction :one
INSERT INTO transactions (
    user_id,
    order_id,
    trade_id,
    type,
    quantity,
    price,
    amount,
    outcome
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING *;
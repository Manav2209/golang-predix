-- name: CreateOrder :one
INSERT INTO orders (id, event_id, user_id, order_type, outcome, side, quantity, price, status)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, 'PENDING')
RETURNING id, event_id, user_id, order_type, outcome, side, quantity, price, status, created_at, updated_at;

-- name: UpdateOrderStatus :exec
UPDATE orders
SET status = $2, updated_at = NOW()
WHERE id = $1;

-- name: GetOrdersByUserAndEvent :many
SELECT id, event_id, user_id, order_type, outcome, side, quantity, price, status, created_at, updated_at
FROM orders
WHERE user_id = $1 AND event_id = $2;

-- name: UpdateOrderFill :exec
UPDATE orders
SET
    status = $2,
    updated_at = NOW()
WHERE id = $1;
WITH
    base AS (
        SELECT
            uuid_generate_v4() AS encodedKey,
            orders.id,
            exploded_data.*
        FROM
            {{ref('stg_order')}} orders
        CROSS JOIN LATERAL json_array_elements(orders.details) AS exploded_data
    )
SELECT
    encodedKey,
    id AS orderID,
    json_extract_path(value, 'productId') AS productId,
    json_extract_path(value, 'unitPrice') AS unitPrice,
    json_extract_path(value, 'quantity') AS quantity,
    json_extract_path(value, 'discount') AS discount
FROM
    base
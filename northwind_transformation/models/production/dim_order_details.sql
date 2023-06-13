WITH
    base AS (
        SELECT
            uuid_generate_v4() AS encodedKey,
            orders.id,
            exploded_data.*
        FROM
            {{ref('stg_order')}} orders
        CROSS JOIN LATERAL json_array_elements(orders.details) AS exploded_data
    ),

    t1 AS (
        SELECT
            encodedKey,
            id AS orderID,
            json_extract_path(value, 'productId') AS productId,
            json_extract_path(value, 'unitPrice') AS unitPrice,
            json_extract_path(value, 'quantity') AS quantity,
            json_extract_path(value, 'discount') AS discount
        FROM
            base
    ),

    t2 AS (
        SELECT
            encodedKey,
            orderID,
            CAST(productId AS VARCHAR) AS productId,
            CAST(unitPrice AS VARCHAR) AS unitPrice,
            CAST(quantity AS VARCHAR) AS quantity,
            CAST(discount AS VARCHAR) AS discount
        FROM
            t1
    ),

    t3 AS (
        SELECT
            encodedKey,
            orderID,
            CAST(productId AS INT) AS productId,
            CAST(unitPrice AS FLOAT) AS unitPrice,
            CAST(quantity AS INT) AS quantity,
            CAST(discount AS FLOAT) AS discount
        FROM
            t2
    )

SELECT
    *
FROM
    t3
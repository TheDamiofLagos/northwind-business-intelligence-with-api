WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id) AS rownum
    FROM
        order_table
)

SELECT
    *
FROM
    base
WHERE
    rownum = 1
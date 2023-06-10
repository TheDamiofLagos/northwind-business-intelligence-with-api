WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id) AS rownum
    FROM
        supplier_table
)

SELECT
    *
FROM
    base
WHERE
    rownum = 1
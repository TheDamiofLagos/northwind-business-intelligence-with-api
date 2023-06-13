WITH
    base_rev AS (
        SELECT
            orderid,
            (unitprice * dim_order_details.quantity) - discount AS revenue
        FROM
            {{ref('dim_order_details')}}
    ),

    rev_order AS (
        SELECT
            orderid,
            SUM(revenue) AS revenuePerOrder
        FROM
            base_rev
        GROUP BY
            1
    )

SELECT
    f.orderid,
    f.orderdate,
    f.shippeddate,
    f.freight,
    ro.revenuePerOrder
FROM
    {{ref('fact_orders')}} f
LEFT JOIN
    rev_order ro
ON
    f.orderid = ro.orderid




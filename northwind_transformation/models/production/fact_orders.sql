SELECT
    id AS orderID,
    customerid,
    employeeid,
    DATE(orderdate) AS orderdate,
    DATE(requireddate) AS requireddate,
    DATE(shippeddate) AS shippeddate,
    shipvia AS shipperID,
    freight
FROM
    {{ ref('stg_order')}}
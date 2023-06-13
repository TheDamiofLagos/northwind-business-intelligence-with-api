WITH
    base AS (
        SELECT
            orderid,
            customerid,
            employeeid,
            orderdate,
            shippeddate,
            requireddate,
            freight,
            shipperid
        FROM
            {{ref('fact_orders')}}
    ),

    product AS (
        SELECT
            productid,
            productname,
            supplierid,
            categoryid,
            quantityperunit,
            unitsinstock,
            unitsonorder,
            reorderlevel,
            discontinued
        FROM
            {{ref('dim_product')}}
    ),

    details AS (
        SELECT
            orderid,
            productid,
            unitprice,
            quantity,
            discount
        FROM
            {{ref('dim_order_details')}}
    ),

    supplier AS (
        SELECT
            supplierid,
            companyname,
            contactname,
            contacttitle,
            city,
            region,
            country
        FROM
            {{ref('dim_supplier')}}
    ),

    customer AS (
        SELECT
            "customerID",
            "companyName" AS customer_CompanyName,
            "contactName" AS customer_ContactPerson,
            "contactTitle" AS customer_ContactTitle,
            city AS customerCity,
            country AS customerCountry
        FROM
            {{ref('dim_customer')}}
    ),

    shipper AS (
        SELECT
            "shipperID",
            "companyName" AS shipperName
        FROM
            {{ref('dim_shipper')}}
    ),

    attendant AS (
        SELECT
            a."employeeID",
            a."employeeName" attendantName,
            a.title AS attendantTitle,
            b."employeeName" managerName,
            b.title managerTitle,
            a.country
        FROM
            {{ref('dim_employee')}} a
        LEFT JOIN
            {{ref('dim_employee')}} b
        ON
            a."reportsTo" = b."employeeID"
    ),

    category AS (
        SELECT
            id AS categoryID,
            name AS categoryName
        FROM
            {{ref('dim_category')}}
    )

SELECT
    uuid_generate_v4() AS encodedKey,
    b.orderid,
    b.orderdate,
    b.shippeddate,
    b.requireddate,
    b.freight,
    d.unitprice,
    d.quantity,
    d.discount,
    p.productname,
    ca.categoryName AS productCategory,
    p.quantityperunit,
    p.unitsinstock,
    p.unitsonorder,
    p.reorderlevel,
    p.discontinued,
    s.companyname AS supplierName,
    s.contactname AS supplierContact,
    s.contacttitle,
    s.city AS supplierCity,
    s.region AS supplierRegion,
    s.country AS supplierCountry,
    c."customerID" AS customerID,
    c.customer_CompanyName,
    c.customer_ContactPerson,
    c.customer_ContactTitle,
    c.customerCity,
    c.customerCountry,
    sh.shipperName AS shippedVia,
    at.attendantName,
    at.attendantTitle,
    at.managerName,
    at.country AS attendantCountry
FROM
    base b
LEFT JOIN
    details d
ON
    b.orderid = d.orderid
LEFT JOIN
    product p
ON
    d.productid = p.productid
LEFT JOIN
    supplier s
ON
    p.supplierid = s.supplierid
LEFT JOIN
    customer c
ON
    b.customerid = c."customerID"
LEFT JOIN
    shipper sh
ON
    b.shipperid = sh."shipperID"
LEFT JOIN
    attendant at
ON
    b.employeeid = at."employeeID"
LEFT JOIN
    category ca
ON
    p.categoryid = ca.categoryID
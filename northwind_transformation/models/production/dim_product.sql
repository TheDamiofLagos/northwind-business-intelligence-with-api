select
    id AS productID,
    name AS productName,
    supplierid,
    categoryid,
    quantityperunit,
    unitprice,
    unitsinstock,
    unitsonorder,
    reorderlevel,
    discontinued
from
    {{ref('stg_product')}}
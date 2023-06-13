WITH base AS (SELECT id,
                     companyname,
                     contactname,
                     contacttitle,
                     json_extract_path(address, 'street')     AS street,
                     json_extract_path(address, 'city')       AS city,
                     json_extract_path(address, 'region')     AS region,
                     json_extract_path(address, 'postalCode') AS postalCode,
                     json_extract_path(address, 'country')    AS country,
                     json_extract_path(address, 'phone')      AS phone
              FROM {{ref('stg_supplier')}}
              ),

     t1 AS (SELECT id                          AS supplierID,
                   companyname,
                   contactname,
                   contacttitle,
                   CAST(street AS VARCHAR)     AS street,
                   CAST(city AS VARCHAR)       AS city,
                   CAST(region AS VARCHAR)     AS region,
                   CAST(postalCode AS VARCHAR) AS postalCode,
                   CAST(country AS VARCHAR)    AS country,
                   CAST(phone AS VARCHAR)      AS phone
            FROM base)
SELECT supplierID,
       companyname,
       contactname,
       contacttitle,
       CASE WHEN UPPER(REPLACE(street, '"', '')) = 'NULL' THEN NULL ELSE REPLACE(street, '"', '') END         AS street,
       CASE WHEN UPPER(REPLACE(city, '"', '')) = 'NULL' THEN NULL ELSE REPLACE(city, '"', '') END             AS city,
       CASE WHEN UPPER(REPLACE(region, '"', '')) = 'NULL' THEN NULL ELSE REPLACE(region, '"', '') END         AS region,
       CASE
           WHEN UPPER(REPLACE(postalCode, '"', '')) = 'NULL' THEN NULL
           ELSE REPLACE(postalCode, '"', '') END                                                              AS postalCode,
       CASE
           WHEN UPPER(REPLACE(country, '"', '')) = 'NULL' THEN NULL
           ELSE REPLACE(country, '"', '') END                                                                 AS country,
       CASE WHEN UPPER(REPLACE(phone, '"', '')) = 'NULL' THEN NULL ELSE REPLACE(phone, '"', '') END           AS phone
FROM t1

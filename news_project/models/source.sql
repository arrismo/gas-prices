select
    SOURCE,
    COUNT(*) AS SOURCE_COUNT
FROM {{ source('public','articledata')}}
GROUP BY SOURCE

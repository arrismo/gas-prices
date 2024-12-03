select 
    SOURCE,
    AUTHOR,
    TITLE,
    DESCRIPTION,
    YEAR(PUBLISH_DATE) as YEAR,
    MONTH(PUBLISH_DATE) as MONTH
from {{ source('public','articledata')}}

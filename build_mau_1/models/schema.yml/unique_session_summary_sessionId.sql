
    
    

select
    sessionId as unique_field,
    count(*) as n_records

from USER_DB_MARMOT.raw.session_summary
where sessionId is not null
group by sessionId
having count(*) > 1



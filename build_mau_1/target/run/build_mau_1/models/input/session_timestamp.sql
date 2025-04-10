
  create or replace   view USER_DB_MARMOT.raw.session_timestamp
  
   as (
    SELECT
    sessionId,
    ts
FROM USER_DB_MARMOT.dev.session_timestamp
  );



  create or replace   view USER_DB_MARMOT.raw.user_session_channel
  
   as (
    SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_MARMOT.dev.user_session_channel
  );


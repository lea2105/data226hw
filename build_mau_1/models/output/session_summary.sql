WITH u AS (
    SELECT * FROM USER_DB_MARMOT.raw.user_session_channel
), st AS (
    SELECT * FROM USER_DB_MARMOT.raw.session_timestamp
)
SELECT u.userId, u.sessionId, u.channel, st.ts
FROM u
JOIN st ON u.sessionId = st.sessionId
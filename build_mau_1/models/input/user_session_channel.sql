SELECT
    userId,
    sessionId,
    channel
FROM {{ source('dev', 'user_session_channel') }}

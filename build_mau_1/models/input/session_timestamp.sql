SELECT
    sessionId,
    ts
FROM {{ source('dev', 'session_timestamp') }}

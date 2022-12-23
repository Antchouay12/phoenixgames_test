/*
Changed the definition of a session to fix:
    - sometimes we got events just before login (eg. user_id = '075bb4533f402accb289c14fa7ea79ba3a9023d92594ffbcc3966792466ce3a5')
To fix this case, we will consider login as a new session only if no events were triggered in the last 5 minutes
    - sometimes we got events without login (eg. user_id = '572d63e0c993151c45412273902ff983ac057fc3bb7d11fea5e6afc1fe68309f')
To fix this case, we will consider any event as the start of a new session

    - sometimes we got missions longer than 15 minutes (eg. user_id = '44fce12b3e91e39c992148b0652ad4d9f363c803693607461902f45ec6237d6e')
To fix this case, we will consider login inactivity as 'no event during 30 minutes'
Some missions are still lasting more than 30 minutes but we'll consider the user stopped the game for a while and that the mission finished during a new session
*/

WITH lag_event_time AS
(
    SELECT
        user_id,
        event_id,
        -- In case of multiple events on the same timestamp, we consider 'login' as the first one
        CASE WHEN event_name = 'login' THEN 0 ELSE 1 END as custom_event_order,
        event_time,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time, custom_event_order, event_id) AS prev_event_time
    FROM "events"
    WHERE event_time IS NOT NULL
),

session_id_creation AS
(
    SELECT
        user_id,
        event_id,
        custom_event_order,
        event_time,
        CASE
            WHEN 
            -- First user event ever
            prev_event_time IS NULL
            -- Event 'login' and last event is older than 15 minutes
            OR (custom_event_order = 0 AND event_time - prev_event_time > INTERVAL '5 minutes')
            -- Any event and last event is older than 30 minutes
            OR event_time - prev_event_time > INTERVAL '30 minutes'

            -- Creating session_id as a MD5() of user_id and first event_time
            THEN MD5(COALESCE(user_id, '') || event_time)
            ELSE NULL
        END as session_id
    FROM lag_event_time
),

session_id_propagation as
(
    SELECT
        LAST_VALUE(session_id IGNORE NULLS) OVER (PARTITION BY user_id ORDER BY event_time, custom_event_order, event_id ROWS UNBOUNDED PRECEDING) as session_id,
        user_id,
        event_time
    FROM session_id_creation
)

SELECT
    session_id,
    user_id,
    MIN(event_time) AS start_time,
    MAX(event_time) AS end_time
FROM session_id_propagation
GROUP BY session_id, user_id
ORDER BY start_time

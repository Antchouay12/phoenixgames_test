-- Deduplication of ipv4 table based on network_integer
WITH ipv4_to_country_deduplication AS
(
    SELECT DISTINCT
        country_iso_code,
        country_name,
        network_start_integer,
        network_last_integer
    FROM ipv4_to_country
),

-- Calculation of network integer based on IPv4
user_networks AS
(
    SELECT
        user_id,
        SPLIT_PART(ip_address, '.', 1)::BIGINT * POWER(256, 3)
        + SPLIT_PART(ip_address, '.', 2)::BIGINT * POWER(256, 2)
        + SPLIT_PART(ip_address, '.', 3)::BIGINT * POWER(256, 1)
        + SPLIT_PART(ip_address, '.', 4)::BIGINT AS network_integer,
        MIN(event_time) AS updated_time
    FROM events
    WHERE event_time IS NOT NULL
    GROUP BY user_id, network_integer
)

-- Merge and user_key creation
SELECT
    MD5(COALESCE(u.user_id, '') || COALESCE(i.country_iso_code, 'Unknown')) as user_key,
    u.user_id,
    COALESCE(i.country_iso_code, 'Unknown') AS country_code,
    MIN(i.country_name) AS country_name,
    MIN(u.updated_time) AS updated_time
FROM user_networks u

LEFT JOIN ipv4_to_country_deduplication i
ON u.network_integer >= i.network_start_integer
AND u.network_integer <= i.network_last_integer

GROUP BY u.user_id, i.country_iso_code

-- ===================================================================
-- Spotify ETL Pipeline - Data Quality & Monitoring Queries
-- ===================================================================
-- Purpose: Daily validation and health checks for Snowflake tables
-- Usage: Run these queries daily to monitor pipeline health
-- Owner: Data Platform Team
-- ===================================================================

USE SCHEMA SPOTIFY_DB.RAW_DATA;

-- ===================================================================
-- Basic Health Checks
-- ===================================================================

-- Row counts across all tables
SELECT 'Albums' AS table_name, COUNT(*) AS row_count FROM tblAlbum
UNION ALL
SELECT 'Artists', COUNT(*) FROM tblArtist
UNION ALL  
SELECT 'Songs', COUNT(*) FROM tblSongs;

-- Record freshness - check latest loaded data
SELECT 
    'Albums' AS table_name,
    MAX(loaded_at) AS last_load_time,
    TIMESTAMPDIFF(MINUTE, MAX(loaded_at), CURRENT_TIMESTAMP()) AS minutes_since_last_load
FROM tblAlbum
UNION ALL
SELECT 
    'Artists',
    MAX(loaded_at),
    TIMESTAMPDIFF(MINUTE, MAX(loaded_at), CURRENT_TIMESTAMP())
FROM tblArtist
UNION ALL
SELECT 
    'Songs',
    MAX(loaded_at),
    TIMESTAMPDIFF(MINUTE, MAX(loaded_at), CURRENT_TIMESTAMP())
FROM tblSongs;

-- ===================================================================
-- Data Quality Checks
-- ===================================================================

-- Check for NULL values in critical fields
SELECT 
    COUNT(*) AS total_songs,
    COUNT(song_name) AS songs_with_name,
    COUNT(album_id) AS songs_with_album,
    COUNT(artist_id) AS songs_with_artist,
    COUNT(url) AS songs_with_url,
    COUNT(song_added) AS songs_with_added_date,
    COUNT(*) - COUNT(song_name) AS null_song_names,
    COUNT(*) - COUNT(url) AS null_urls
FROM tblSongs;

-- Duplicate detection
SELECT 
    'Albums' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT album_id) AS unique_records,
    COUNT(*) - COUNT(DISTINCT album_id) AS duplicates
FROM tblAlbum
UNION ALL
SELECT 
    'Artists',
    COUNT(*),
    COUNT(DISTINCT artist_id),
    COUNT(*) - COUNT(DISTINCT artist_id)
FROM tblArtist
UNION ALL
SELECT 
    'Songs',
    COUNT(*),
    COUNT(DISTINCT song_id),
    COUNT(*) - COUNT(DISTINCT song_id)
FROM tblSongs;

-- ===================================================================
-- Referential Integrity Checks
-- ===================================================================

-- Orphan songs - songs without valid album reference
SELECT COUNT(*) AS orphan_songs_missing_album
FROM tblSongs s
LEFT JOIN tblAlbum a ON s.album_id = a.album_id
WHERE a.album_id IS NULL;

-- Orphan songs - songs without valid artist reference
SELECT COUNT(*) AS orphan_songs_missing_artist
FROM tblSongs s
LEFT JOIN tblArtist ar ON s.artist_id = ar.artist_id
WHERE ar.artist_id IS NULL;

-- Detailed orphan records (for investigation)
SELECT 
    s.song_id,
    s.song_name,
    s.album_id AS referenced_album_id,
    s.artist_id AS referenced_artist_id,
    CASE WHEN a.album_id IS NULL THEN 'Missing' ELSE 'OK' END AS album_status,
    CASE WHEN ar.artist_id IS NULL THEN 'Missing' ELSE 'OK' END AS artist_status
FROM tblSongs s
LEFT JOIN tblAlbum a ON s.album_id = a.album_id
LEFT JOIN tblArtist ar ON s.artist_id = ar.artist_id
WHERE a.album_id IS NULL OR ar.artist_id IS NULL
LIMIT 10;

-- ===================================================================
-- Snowpipe Monitoring
-- ===================================================================

-- Snowpipe status
SELECT 
    'pipe_albums' AS pipe_name,
    SYSTEM$PIPE_STATUS('pipe_albums') AS status
UNION ALL
SELECT 
    'pipe_artists',
    SYSTEM$PIPE_STATUS('pipe_artists')
UNION ALL
SELECT 
    'pipe_songs',
    SYSTEM$PIPE_STATUS('pipe_songs');

-- Recent load history - last 24 hours
SELECT 
    TABLE_NAME,
    FILE_NAME,
    LAST_LOAD_TIME,
    STATUS,
    ROW_COUNT,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLSONGS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;

-- Load errors - failures in last 7 days
SELECT 
    TABLE_NAME,
    FILE_NAME,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLSONGS',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED' OR ERROR_COUNT > 0
ORDER BY LAST_LOAD_TIME DESC;

-- Daily load summary
SELECT 
    DATE(LAST_LOAD_TIME) AS load_date,
    COUNT(DISTINCT FILE_NAME) AS files_loaded,
    SUM(ROW_COUNT) AS total_rows_loaded,
    SUM(ERROR_COUNT) AS total_errors,
    AVG(ROW_COUNT) AS avg_rows_per_file
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLSONGS',
    START_TIME => DATEADD(days, -30, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOADED'
GROUP BY DATE(LAST_LOAD_TIME)
ORDER BY load_date DESC;

-- ===================================================================
-- Pipeline Performance Metrics
-- ===================================================================

-- End-to-end latency - extraction to load
SELECT 
    DATE(extracted_at) AS date,
    AVG(TIMESTAMPDIFF(MINUTE, extracted_at, transformed_at)) AS extract_to_transform_mins,
    AVG(TIMESTAMPDIFF(MINUTE, transformed_at, loaded_at)) AS transform_to_load_mins,
    AVG(TIMESTAMPDIFF(MINUTE, extracted_at, loaded_at)) AS total_latency_mins,
    COUNT(*) AS records_processed
FROM tblSongs
WHERE extracted_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY DATE(extracted_at)
ORDER BY date DESC;

-- Records processed per day
SELECT 
    DATE(loaded_at) AS load_date,
    COUNT(*) AS songs_loaded,
    COUNT(DISTINCT album_id) AS unique_albums,
    COUNT(DISTINCT artist_id) AS unique_artists
FROM tblSongs
WHERE loaded_at >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY DATE(loaded_at)
ORDER BY load_date DESC;

-- ===================================================================
-- Business Analytics Health
-- ===================================================================

-- Album distribution by type
SELECT 
    album_type,
    COUNT(*) AS album_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM tblAlbum
GROUP BY album_type
ORDER BY album_count DESC;

-- Top 10 most popular songs
SELECT 
    s.song_name,
    ar.artist_name,
    al.album_name,
    s.popularity,
    s.url AS spotify_link
FROM tblSongs s
JOIN tblArtist ar ON s.artist_id = ar.artist_id
JOIN tblAlbum al ON s.album_id = al.album_id
ORDER BY s.popularity DESC
LIMIT 10;

-- Artists with most songs in playlist
SELECT 
    ar.artist_name,
    COUNT(s.song_id) AS song_count,
    AVG(s.popularity) AS avg_popularity
FROM tblArtist ar
JOIN tblSongs s ON ar.artist_id = s.artist_id
GROUP BY ar.artist_name
ORDER BY song_count DESC
LIMIT 10;

-- ===================================================================
-- Alerting Thresholds
-- ===================================================================

-- Data freshness alert - no data loaded in last 2 hours
SELECT 
    CASE 
        WHEN MAX(loaded_at) < DATEADD(hour, -2, CURRENT_TIMESTAMP()) 
        THEN 'ALERT: No data loaded in last 2 hours'
        ELSE 'OK'
    END AS freshness_status,
    MAX(loaded_at) AS last_load_time,
    TIMESTAMPDIFF(MINUTE, MAX(loaded_at), CURRENT_TIMESTAMP()) AS minutes_since_load
FROM tblSongs;

-- Orphan records alert - more than 0 orphans
SELECT 
    CASE 
        WHEN COUNT(*) > 0 
        THEN CONCAT('ALERT: ', COUNT(*), ' orphan songs detected')
        ELSE 'OK'
    END AS integrity_status,
    COUNT(*) AS orphan_count
FROM tblSongs s
LEFT JOIN tblAlbum a ON s.album_id = a.album_id
WHERE a.album_id IS NULL;

-- Load failure alert - errors in last 24 hours
SELECT 
    CASE 
        WHEN SUM(ERROR_COUNT) > 0 
        THEN CONCAT('ALERT: ', SUM(ERROR_COUNT), ' load errors in last 24 hours')
        ELSE 'OK'
    END AS load_status,
    SUM(ERROR_COUNT) AS total_errors,
    COUNT(DISTINCT FILE_NAME) AS failed_files
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLSONGS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED' OR ERROR_COUNT > 0;

-- ===================================================================
-- END OF MONITORING QUERIES
-- ===================================================================

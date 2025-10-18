-- ===================================================================
-- Spotify ETL Pipeline - Snowflake Data Warehouse
-- ===================================================================
-- Purpose: Create tables, stages, and automated ingestion pipeline
-- Owner: Data Platform Team
-- Created: 2025-10-18
-- 
-- Execution Order:
--   1. Database & Schema Setup
--   2. Table Definitions
--   3. Storage Integration & Stage
--   4. File Format & Initial Load
--   5. Snowpipe Configuration
--
-- Dependencies:
--   - AWS S3 bucket: spotify-etl-project-mang
--   - AWS IAM role: spotify-snowflake-s3-connection
--   - Snowflake role: ACCOUNTADMIN (for storage integration)
-- ===================================================================

-- ===================================================================
-- SECTION 1: Database & Schema Setup
-- ===================================================================

CREATE OR REPLACE DATABASE SPOTIFY_DB
    COMMENT = 'Spotify playlist analytics data warehouse';

CREATE OR REPLACE SCHEMA SPOTIFY_DB.RAW_DATA
    COMMENT = 'Raw data layer - S3 ingestion via Snowpipe';

USE SCHEMA SPOTIFY_DB.RAW_DATA;

-- ===================================================================
-- SECTION 2: Table Definitions
-- ===================================================================

-- Album dimension table - parent for songs relationship
CREATE OR REPLACE TABLE tblAlbum (
    album_id VARCHAR(22) PRIMARY KEY,
    album_name VARCHAR(500),
    release_date DATE,
    total_tracks INTEGER,
    album_url VARCHAR(500),
    album_type VARCHAR(50),        -- single/album/compilation
    label VARCHAR(500),
    extracted_at TIMESTAMP_NTZ,    -- API extraction timestamp
    transformed_at TIMESTAMP_NTZ,  -- CSV transformation timestamp
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Dimension table - Album master data';

-- Artist dimension table - parent for songs relationship
CREATE OR REPLACE TABLE tblArtist (
    artist_id VARCHAR(22) PRIMARY KEY,
    artist_name VARCHAR(500) NOT NULL,
    artist_url VARCHAR(500),
    extracted_at TIMESTAMP_NTZ,
    transformed_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Dimension table - Artist master data';

-- Songs fact table - references albums and artists
CREATE OR REPLACE TABLE tblSongs (
    song_id VARCHAR(50) PRIMARY KEY,
    song_name VARCHAR(500) NOT NULL,
    duration_ms INTEGER,
    url VARCHAR(1000),
    popularity INTEGER,             -- Spotify popularity score 0-100
    song_added TIMESTAMP_NTZ,       -- When added to playlist
    album_id VARCHAR(50) NOT NULL,  -- FK to tblAlbum
    artist_id VARCHAR(50) NOT NULL, -- FK to tblArtist
    extracted_at TIMESTAMP_NTZ,
    transformed_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Fact table - Song details with album and artist references';

-- Verify tables created
SHOW TABLES LIKE 'tbl%';

-- ===================================================================
-- SECTION 3: Storage Integration & External Stage
-- ===================================================================

-- Storage integration for S3 access
-- Requires ACCOUNTADMIN privileges to create
CREATE OR REPLACE STORAGE INTEGRATION s3_spotify_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::588738604241:role/spotify-snowflake-s3-connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-mang/transformed_data/')
    COMMENT = 'AWS S3 integration for Spotify transformed data';

-- Retrieve external ID and IAM user ARN for AWS trust policy configuration
DESC STORAGE INTEGRATION s3_spotify_integration;

-- External stage pointing to S3 transformed data
CREATE OR REPLACE STAGE s3_spotify_stage
    STORAGE_INTEGRATION = s3_spotify_integration
    URL = 's3://spotify-etl-project-mang/transformed_data/'
    COMMENT = 'External stage for S3 transformed CSV files';

-- Verify S3 connectivity
LIST @s3_spotify_stage/album_data/;
LIST @s3_spotify_stage/artist_data/;
LIST @s3_spotify_stage/song_data/;

-- ===================================================================
-- SECTION 4: File Format & Initial Data Load
-- ===================================================================

-- CSV file format for all data files
CREATE OR REPLACE FILE FORMAT fmt_csv_spotify
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', 'NaN')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMMENT = 'Standard CSV format for Spotify data files';

-- Preview sample data before loading
SELECT 
    $1 AS album_id,
    $2 AS album_name,
    $3 AS release_date,
    $4 AS total_tracks,
    $5 AS album_url,
    $6 AS album_type,
    $7 AS label,
    $8 AS extracted_at,
    $9 AS transformed_at
FROM @s3_spotify_stage/album_data/ (FILE_FORMAT => fmt_csv_spotify) 
LIMIT 5;

-- Initial bulk load - Albums
-- Run first as parent table for songs FK
COPY INTO tblAlbum (
    album_id,
    album_name,
    release_date,
    total_tracks,
    album_url,
    album_type,
    label,
    extracted_at,
    transformed_at
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
    FROM @s3_spotify_stage/album_data/
)
FILE_FORMAT = fmt_csv_spotify
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';

-- Initial bulk load - Artists
-- Run second as parent table for songs FK
COPY INTO tblArtist (
    artist_id,
    artist_name,
    artist_url,
    extracted_at,
    transformed_at
)
FROM (
    SELECT $1, $2, $3, $4, $5
    FROM @s3_spotify_stage/artist_data/
)
FILE_FORMAT = fmt_csv_spotify
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';

-- Initial bulk load - Songs
-- Run last as child table with FK dependencies
COPY INTO tblSongs (
    song_id,
    song_name,
    duration_ms,
    url,
    popularity,
    song_added,
    album_id,
    artist_id,
    extracted_at,
    transformed_at
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
    FROM @s3_spotify_stage/song_data/
)
FILE_FORMAT = fmt_csv_spotify
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE';

-- Verify load results
SELECT 'Albums' AS table_name, COUNT(*) AS row_count FROM tblAlbum
UNION ALL
SELECT 'Artists', COUNT(*) FROM tblArtist
UNION ALL  
SELECT 'Songs', COUNT(*) FROM tblSongs;

-- ===================================================================
-- SECTION 5: Snowpipe Configuration (Auto-Ingestion)
-- ===================================================================

-- Snowpipe for albums
-- Configure S3 event notifications using notification_channel ARN below
CREATE OR REPLACE PIPE pipe_albums
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipeline for album data'
AS
COPY INTO tblAlbum (
    album_id,
    album_name,
    release_date,
    total_tracks,
    album_url,
    album_type,
    label,
    extracted_at,
    transformed_at
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
    FROM @s3_spotify_stage/album_data/
)
FILE_FORMAT = fmt_csv_spotify
ON_ERROR = 'CONTINUE';

-- Snowpipe for artists
CREATE OR REPLACE PIPE pipe_artists
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipeline for artist data'
AS
COPY INTO tblArtist (
    artist_id,
    artist_name,
    artist_url,
    extracted_at,
    transformed_at
)
FROM (
    SELECT $1, $2, $3, $4, $5
    FROM @s3_spotify_stage/artist_data/
)
FILE_FORMAT = fmt_csv_spotify
ON_ERROR = 'CONTINUE';

-- Snowpipe for songs
CREATE OR REPLACE PIPE pipe_songs
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipeline for song data'
AS
COPY INTO tblSongs (
    song_id,
    song_name,
    duration_ms,
    url,
    popularity,
    song_added,
    album_id,
    artist_id,
    extracted_at,
    transformed_at
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
    FROM @s3_spotify_stage/song_data/
)
FILE_FORMAT = fmt_csv_spotify
ON_ERROR = 'CONTINUE';

-- Retrieve SQS queue ARNs for S3 event notification configuration
-- Copy notification_channel values to AWS S3 bucket event settings
DESC PIPE pipe_albums;
DESC PIPE pipe_artists;
DESC PIPE pipe_songs;

-- Check Snowpipe status
SELECT SYSTEM$PIPE_STATUS('pipe_albums');
SELECT SYSTEM$PIPE_STATUS('pipe_artists');
SELECT SYSTEM$PIPE_STATUS('pipe_songs');

-- ===================================================================
-- END OF DDL SCRIPT
-- ===================================================================
-- Next Steps:
--   1. Configure AWS S3 event notifications (see deployment guide)
--   2. Run validation queries (see monitoring/validation_queries.sql)
--   3. Monitor Snowpipe execution (see monitoring queries)
-- ===================================================================

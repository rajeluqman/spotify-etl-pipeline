# Snowflake Data Warehouse Setup

Complete Snowflake configuration for Spotify ETL pipeline with automated S3 ingestion via Snowpipe.

## Overview

This directory contains all SQL scripts and documentation for deploying and managing the Snowflake data warehouse component of the Spotify analytics pipeline.

### Architecture

```
┌─────────────┐
│   AWS S3    │
│transformed_ │
│   data/     │
└──────┬──────┘
       │ S3 Event Notification
       ▼
┌─────────────┐
│  Snowpipe   │
│Auto-Ingestion│
└──────┬──────┘
       │ COPY INTO
       ▼
┌─────────────┐
│  Snowflake  │
│   Tables    │
└─────────────┘
```

### Data Model

**Star Schema Design:**

```
          tblAlbum (Dimension)
               ↑
               │
          tblSongs (Fact) ← tblArtist (Dimension)
```

**Tables:**
- `tblAlbum` - Album master data (dimension)
- `tblArtist` - Artist master data (dimension)
- `tblSongs` - Song details with popularity metrics (fact)

## Quick Start

### Prerequisites

- Snowflake account (ACCOUNTADMIN role)
- AWS account with S3 access
- CSV files in S3: `s3://spotify-etl-project-mang/transformed_data/`

### 5-Minute Setup

```sql
-- 1. Run DDL script
-- File: ddl/spotify_snowflake_ddl.sql
-- Creates: Database, tables, storage integration, Snowpipes

-- 2. Configure AWS IAM
-- Follow: deployment/setup_guide.md (Step 1-2)

-- 3. Configure S3 event notifications
-- Follow: deployment/setup_guide.md (Step 4)

-- 4. Verify data loading
SELECT COUNT(*) FROM tblSongs;
```

## Project Structure

```
snowflake/
├── README.md                       # This file
├── ddl/
│   └── spotify_snowflake_ddl.sql  # Table schemas, Snowpipe setup
├── monitoring/
│   └── validation_queries.sql     # Health checks, data quality
└── deployment/
    └── setup_guide.md             # Step-by-step deployment
```

## File Descriptions

### DDL Scripts

**`ddl/spotify_snowflake_ddl.sql`**
- Database and schema creation
- Table definitions with timestamps
- S3 storage integration
- File format configuration
- Initial data load (COPY INTO)
- Snowpipe auto-ingestion setup

**Execution time:** ~5 minutes

### Monitoring Queries

**`monitoring/validation_queries.sql`**
- Row count checks
- Data freshness validation
- Referential integrity checks
- Snowpipe execution history
- Pipeline performance metrics
- Alerting thresholds

**Usage:** Run daily for health monitoring

### Deployment Guide

**`deployment/setup_guide.md`**
- Complete setup checklist
- AWS IAM role configuration
- Snowflake storage integration
- S3 event notification setup
- Troubleshooting guide
- Cost optimization tips

## Table Schemas

### tblAlbum (Dimension)

| Column | Type | Description |
|--------|------|-------------|
| `album_id` | VARCHAR(22) PK | Spotify album ID |
| `album_name` | VARCHAR(500) | Album title |
| `release_date` | DATE | Release date |
| `total_tracks` | INTEGER | Number of tracks |
| `album_url` | VARCHAR(500) | Spotify URL |
| `album_type` | VARCHAR(50) | single/album/compilation |
| `label` | VARCHAR(500) | Record label |
| `extracted_at` | TIMESTAMP_NTZ | API extraction time |
| `transformed_at` | TIMESTAMP_NTZ | CSV transformation time |
| `loaded_at` | TIMESTAMP_NTZ | Snowflake load time |

### tblArtist (Dimension)

| Column | Type | Description |
|--------|------|-------------|
| `artist_id` | VARCHAR(22) PK | Spotify artist ID |
| `artist_name` | VARCHAR(500) | Artist name |
| `artist_url` | VARCHAR(500) | Spotify URL |
| `extracted_at` | TIMESTAMP_NTZ | API extraction time |
| `transformed_at` | TIMESTAMP_NTZ | CSV transformation time |
| `loaded_at` | TIMESTAMP_NTZ | Snowflake load time |

### tblSongs (Fact)

| Column | Type | Description |
|--------|------|-------------|
| `song_id` | VARCHAR(50) PK | Spotify track ID |
| `song_name` | VARCHAR(500) | Track title |
| `duration_ms` | INTEGER | Duration in milliseconds |
| `url` | VARCHAR(1000) | Spotify track URL |
| `popularity` | INTEGER | Popularity score (0-100) |
| `song_added` | TIMESTAMP_NTZ | Added to playlist date |
| `album_id` | VARCHAR(50) FK | Reference to tblAlbum |
| `artist_id` | VARCHAR(50) FK | Reference to tblArtist |
| `extracted_at` | TIMESTAMP_NTZ | API extraction time |
| `transformed_at` | TIMESTAMP_NTZ | CSV transformation time |
| `loaded_at` | TIMESTAMP_NTZ | Snowflake load time |

## Data Flow

```
1. Lambda Transformation
   └─> CSV files saved to S3
       └─> s3://spotify-etl-project-mang/transformed_data/
           ├── album_data/*.csv
           ├── artist_data/*.csv
           └── song_data/*.csv

2. S3 Event Notification
   └─> Triggers SQS queue

3. Snowpipe
   └─> Reads SQS queue
       └─> Executes COPY INTO
           └─> Loads data to Snowflake tables

4. Data Available
   └─> Ready for analytics queries
```

**Latency:** Typically 1-2 minutes from CSV upload to queryable data.

## Deployment

### Full Deployment Guide

See [`deployment/setup_guide.md`](deployment/setup_guide.md) for complete step-by-step instructions.

### Quick Commands

```sql
-- Create database and tables
@ddl/spotify_snowflake_ddl.sql (SECTION 1-2)

-- Setup S3 integration
@ddl/spotify_snowflake_ddl.sql (SECTION 3)

-- Initial data load
@ddl/spotify_snowflake_ddl.sql (SECTION 4)

-- Configure Snowpipes
@ddl/spotify_snowflake_ddl.sql (SECTION 5)

-- Verify setup
@monitoring/validation_queries.sql
```

## Monitoring

### Daily Health Checks

Run these queries from `monitoring/validation_queries.sql`:

```sql
-- Row counts
SELECT 'Albums' AS table_name, COUNT(*) FROM tblAlbum
UNION ALL
SELECT 'Artists', COUNT(*) FROM tblArtist
UNION ALL  
SELECT 'Songs', COUNT(*) FROM tblSongs;

-- Data freshness
SELECT MAX(loaded_at) FROM tblSongs;

-- Orphan records
SELECT COUNT(*) FROM tblSongs s
LEFT JOIN tblAlbum a ON s.album_id = a.album_id
WHERE a.album_id IS NULL;
```

### Snowpipe Status

```sql
SELECT SYSTEM$PIPE_STATUS('pipe_albums');
SELECT SYSTEM$PIPE_STATUS('pipe_artists');
SELECT SYSTEM$PIPE_STATUS('pipe_songs');
```

### Load History

```sql
SELECT 
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ERROR_COUNT,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLSONGS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

## Sample Analytics Queries

### Top 10 Most Popular Songs

```sql
SELECT 
    s.song_name,
    ar.artist_name,
    al.album_name,
    s.popularity,
    s.url
FROM tblSongs s
JOIN tblArtist ar ON s.artist_id = ar.artist_id
JOIN tblAlbum al ON s.album_id = al.album_id
ORDER BY s.popularity DESC
LIMIT 10;
```

### Pipeline Performance

```sql
SELECT 
    DATE(extracted_at) AS date,
    AVG(TIMESTAMPDIFF(MINUTE, extracted_at, loaded_at)) AS total_latency_mins,
    COUNT(*) AS records_processed
FROM tblSongs
WHERE extracted_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY DATE(extracted_at)
ORDER BY date DESC;
```

### Album Distribution by Type

```sql
SELECT 
    album_type,
    COUNT(*) AS album_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM tblAlbum
GROUP BY album_type
ORDER BY album_count DESC;
```

## Cost Estimates

| Component | Usage | Cost/Month |
|-----------|-------|-----------|
| **Storage** | ~500 MB | $0.01 |
| **Snowpipe** | 30 files/day | ~$0.05 |
| **Compute** | Minimal queries | ~$0.10 (free tier) |
| **Total** | | **~$0.16/month** |

### Cost Optimization

```sql
-- Auto-suspend warehouse after 1 minute
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;

-- Auto-resume on query
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = TRUE;
```

## Troubleshooting

### Common Issues

**1. Files not loading**
- Check S3 event notification configured
- Verify SQS queue ARN correct
- Check Snowpipe status

**2. Access denied to S3**
- Verify IAM trust policy
- Check external ID matches
- Confirm role permissions

**3. NULL values in data**
- Already handled by `fmt_csv_spotify`
- Check transformation Lambda output

See [`deployment/setup_guide.md`](deployment/setup_guide.md#troubleshooting) for detailed troubleshooting.

## Dependencies

### Upstream

- **Extraction Lambda:** Provides raw JSON from Spotify API
- **Transformation Lambda:** Converts JSON to CSV files
- **AWS S3:** Stores transformed CSV files

### Downstream

- **Analytics Tools:** Tableau, Power BI, Looker
- **Data Science:** Python notebooks, R scripts
- **Business Intelligence:** SQL queries, dashboards

## Security

### Access Control

```sql
-- Create read-only role for analysts
CREATE ROLE SPOTIFY_ANALYST;
GRANT USAGE ON DATABASE SPOTIFY_DB TO ROLE SPOTIFY_ANALYST;
GRANT USAGE ON SCHEMA SPOTIFY_DB.RAW_DATA TO ROLE SPOTIFY_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SPOTIFY_DB.RAW_DATA TO ROLE SPOTIFY_ANALYST;
```

### Data Privacy

- No PII collected (only public Spotify data)
- URLs are public Spotify links
- Artist/album names are public information

## Performance

### Query Performance

- **Primary Keys:** Indexed automatically for fast lookups
- **Typical query latency:** < 100ms for single row lookups
- **Join performance:** Star schema optimized for analytics

### Load Performance

- **CSV processing:** ~1000 rows/second
- **Snowpipe latency:** 1-2 minutes from S3 upload
- **Concurrent loading:** All 3 Snowpipes run in parallel

## Future Enhancements

- [ ] Add clustering keys for large datasets (>1M rows)
- [ ] Implement materialized views for common queries
- [ ] Create dbt models for data transformations
- [ ] Add SCD Type 2 for artist/album history tracking
- [ ] Setup automated alerting via email/Slack
- [ ] Create Tableau dashboards

## Support

**Documentation:**
- Snowflake: https://docs.snowflake.com
- Storage Integration: https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
- Snowpipe: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro

**Issues?**
- Check monitoring queries for data quality
- Review deployment guide troubleshooting section
- Verify AWS IAM and S3 configurations

---

**Last Updated:** 2025-10-18

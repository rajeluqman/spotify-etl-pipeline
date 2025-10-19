#  Spotify ETL Pipeline

> End-to-end automated data pipeline for Spotify playlist analytics using AWS Lambda, S3, and Snowflake

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange.svg)](https://aws.amazon.com/lambda/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8.svg)](https://www.snowflake.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

##  Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Data Model](#data-model)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [Sample Analytics](#sample-analytics)
- [Cost Estimates](#cost-estimates)

##  Overview

Automated serverless ETL pipeline that extracts Spotify playlist data via API, transforms it into normalized tables, and loads it into Snowflake data warehouse for business intelligence and analytics.

**Key Metrics:**
- ⚡ **Near real-time ingestion** - Data available within 5 minutes
-  **Star schema design** - Optimized for analytics queries
-  **Fully automated** - Serverless architecture with event-driven triggers
-  **Cost-efficient** - ~$0.02/month using AWS free tier

## ️ Architecture

```
![Architecture Diagram](https://github.com/user-attachments/assets/5f45892a-9055-492d-89fa-563a3c00ee9c)
```

### Data Flow

1. **Extract** - Lambda hits Spotify API every 24 hours (EventBridge schedule)
2. **Store Raw** - JSON saved to S3 (`raw_data/to_processed/`)
3. **Transform** - Lambda converts JSON → normalized CSV files
4. **Load** - Snowpipe auto-ingests CSV into Snowflake tables
5. **Archive** - Processed files moved to `already_processed/`

## ✨ Features

- ✅ **Automated Extraction** - Scheduled Lambda via EventBridge
- ✅ **Data Validation** - Deduplication, NULL handling, referential integrity
- ✅ **Error Handling** - Comprehensive logging with execution timestamps
- ✅ **Incremental Loading** - Only new/updated tracks processed
- ✅ **Real-time Monitoring** - CloudWatch metrics and Snowflake validation queries
- ✅ **Cost Optimization** - Serverless architecture, pay-per-use model
- ✅ **Scalable Design** - Handles 10K+ tracks per playlist

## ️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | Spotify Web API | Extract playlist metadata |
| **Extraction** | AWS Lambda (Python 3.9) | Serverless data extraction |
| **Transformation** | AWS Lambda (Python 3.9) | Data normalization |
| **Storage** | AWS S3 | Data lake (raw & transformed) |
| **Orchestration** | AWS EventBridge | Scheduled triggers |
| **Data Warehouse** | Snowflake | Analytics platform |
| **Auto-Ingestion** | Snowpipe | Event-driven loading |
| **Monitoring** | CloudWatch Logs | Execution tracking |

**Python Libraries:**
- `spotipy` - Spotify API client
- `boto3` - AWS SDK
- `pandas` - Data transformation

##  Project Structure

```
spotify-etl-pipeline/
├── README.md                          # This file
├── .gitignore                         # Git ignore rules
├── .env.example                       # Environment variables template
├── requirements.txt                   # Root dependencies
│
├── lambda/
│   ├── extraction/
│   │   ├── lambda_function.py        # Spotify API extraction
│   │   ├── requirements.txt          # Lambda dependencies
│   │   └── README.md                 # Extraction setup guide
│   └── transformation/
│       ├── lambda_function.py        # JSON to CSV transformation
│       ├── requirements.txt          # Lambda dependencies
│       └── README.md                 # Transformation setup guide
│
├── snowflake/
│   ├── README.md                     # Snowflake overview
│   ├── ddl/
│   │   └── spotify_snowflake_ddl.sql # Table schemas & Snowpipe
│   ├── monitoring/
│   │   └── validation_queries.sql    # Health checks
│   └── deployment/
│       └── setup_guide.md            # Deployment instructions
│
├── logs/                             # Execution logs (git-ignored)
├── raw_data/                         # Raw JSON (git-ignored)
└── transformed_data/                 # CSV files (git-ignored)
```

##  Quick Start

### Prerequisites

- AWS Account (Lambda, S3, EventBridge)
- Snowflake Account (free trial available)
- Spotify Developer Account ([create here](https://developer.spotify.com))
- Python 3.9+

### 1. Clone Repository

```bash
git clone https://github.com/rajeluqman/spotify-etl-pipeline.git
cd spotify-etl-pipeline
```

### 2. Configure Credentials

```bash
cp .env.example .env
nano .env  # Add your API keys
```

### 3. Deploy Components

**Lambda Functions:**
- See [`lambda/extraction/README.md`](lambda/extraction/README.md)
- See [`lambda/transformation/README.md`](lambda/transformation/README.md)

**Snowflake:**
- See [`snowflake/deployment/setup_guide.md`](snowflake/deployment/setup_guide.md)

### 4. Test Pipeline

```bash
# Trigger extraction Lambda
aws lambda invoke \
  --function-name spotify-extraction \
  --payload '{"playlist_id":"69kJ5kOz2mKHTdg9FLHVVF"}' \
  response.json
```

##  Data Model

### Star Schema Design

```
          tblAlbum (Dimension)
               ↑
               │
          tblSongs (Fact) ← tblArtist (Dimension)
```

### Tables

**tblAlbum** - Album master data
- `album_id` (PK), `album_name`, `release_date`, `total_tracks`, `album_type`, `label`

**tblArtist** - Artist master data
- `artist_id` (PK), `artist_name`, `artist_url`

**tblSongs** - Song details with metrics
- `song_id` (PK), `song_name`, `duration_ms`, `popularity`, `album_id` (FK), `artist_id` (FK)

All tables include timestamps: `extracted_at`, `transformed_at`, `loaded_at`

##  Deployment

### AWS Lambda Deployment

```bash
# Package extraction Lambda
cd lambda/extraction
pip install -r requirements.txt -t package/
cd package && zip -r ../function.zip .
cd .. && zip function.zip lambda_function.py

# Deploy
aws lambda create-function \
  --function-name spotify-extraction \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-role
```

### Snowflake Setup

```sql
-- Run complete DDL
@snowflake/ddl/spotify_snowflake_ddl.sql
```

See complete guide: [`snowflake/deployment/setup_guide.md`](snowflake/deployment/setup_guide.md)

##  Monitoring

### Health Checks

```sql
-- Row counts
SELECT 'Albums' AS table_name, COUNT(*) FROM tblAlbum
UNION ALL
SELECT 'Artists', COUNT(*) FROM tblArtist
UNION ALL  
SELECT 'Songs', COUNT(*) FROM tblSongs;

-- Data freshness
SELECT MAX(loaded_at) AS last_load FROM tblSongs;
```

### Pipeline Metrics

```sql
-- End-to-end latency
SELECT 
    AVG(TIMESTAMPDIFF(MINUTE, extracted_at, loaded_at)) AS avg_latency_mins
FROM tblSongs
WHERE extracted_at >= DATEADD(day, -7, CURRENT_DATE());
```

See all monitoring queries: [`snowflake/monitoring/validation_queries.sql`](snowflake/monitoring/validation_queries.sql)

##  Sample Analytics

### Top 10 Most Popular Songs

```sql
SELECT 
    s.song_name,
    ar.artist_name,
    al.album_name,
    s.popularity
FROM tblSongs s
JOIN tblArtist ar ON s.artist_id = ar.artist_id
JOIN tblAlbum al ON s.album_id = al.album_id
ORDER BY s.popularity DESC
LIMIT 10;
```

### Album Distribution

```sql
SELECT 
    album_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM tblAlbum
GROUP BY album_type;
```

##  Cost Estimates

| Service | Usage | Cost/Month |
|---------|-------|-----------|
| **Lambda Extraction** | 30 invocations × 30s | $0.00 (Free tier) |
| **Lambda Transformation** | 30 invocations × 30s | $0.00 (Free tier) |
| **S3 Storage** | ~500 MB | $0.01 |
| **Snowflake** | Minimal queries | $0.01 |
| **Total** | | **~$0.02/month** |

##  License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

##  Acknowledgments

- [Spotify Web API](https://developer.spotify.com/documentation/web-api)
- [Snowflake Documentation](https://docs.snowflake.com)
- AWS Lambda for serverless architecture


---

⭐ **Star this repo** if you find it helpful!

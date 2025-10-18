# Spotify Transformation Lambda

## Status: ✅ Production Ready

AWS Lambda function that transforms raw Spotify JSON data into normalized CSV files for data warehouse loading.

## Features

- ✅ Parse JSON playlist data from S3 (`raw_data/to_processed/`)
- ✅ Normalize into 3 entity tables: Albums, Artists, Songs
- ✅ Export CSV files to S3 (`transformed_data/`)
- ✅ Deduplication logic (remove duplicate albums, artists, songs)
- ✅ Data validation with error logging
- ✅ Timestamp tracking (extracted_at, transformed_at)
- ✅ Archive processed files to `already_processed/` folder
- ✅ Handles NULL values and missing fields

## Tech Stack

- **Runtime:** Python 3.9
- **Libraries:** pandas, boto3, python-dotenv
- **AWS Services:** Lambda, S3, CloudWatch

## Data Flow

```
S3: raw_data/to_processed/
    └── playlist_XXX_timestamp.json
         ↓
    [Lambda Transformation]
         ↓
    ┌────────────┬─────────────┬─────────────┐
    ↓            ↓             ↓
album_XXX.csv  artist_XXX.csv  song_XXX.csv
    ↓            ↓             ↓
S3: transformed_data/album_data/
S3: transformed_data/artist_data/
S3: transformed_data/song_data/
         ↓
    [Original JSON moved to]
S3: raw_data/already_processed/
```

## Input Format

**Source:** `raw_data/to_processed/playlist_69kJ5kOz2mKHTdg9FLHVVF_20251018_120000.json`

**JSON Structure:**
```json
{
  "playlist_id": "69kJ5kOz2mKHTdg9FLHVVF",
  "playlist_name": "Your Playlist",
  "extracted_at": "2025-10-18T12:00:00Z",
  "tracks": [
    {
      "track": {
        "id": "abc123",
        "name": "Song Name",
        "duration_ms": 180000,
        "popularity": 85,
        "external_urls": {"spotify": "https://..."},
        "album": {
          "id": "def456",
          "name": "Album Name",
          "release_date": "2024-01-01",
          "total_tracks": 12,
          "album_type": "album",
          "label": "Record Label",
          "external_urls": {"spotify": "https://..."}
        },
        "artists": [
          {
            "id": "xyz789",
            "name": "Artist Name",
            "external_urls": {"spotify": "https://..."}
          }
        ]
      },
      "added_at": "2025-10-01T10:30:00Z"
    }
  ]
}
```

## Output Format

### Albums CSV
**Location:** `transformed_data/album_data/album_20251018_120000.csv`

**Columns:**
- `album_id` - Spotify album ID (22 chars)
- `album_name` - Album title
- `release_date` - Release date (YYYY-MM-DD)
- `total_tracks` - Number of tracks in album
- `album_url` - Spotify URL
- `album_type` - Type: single/album/compilation
- `label` - Record label name
- `extracted_at` - Timestamp when extracted from API
- `transformed_at` - Timestamp when transformed to CSV

### Artists CSV
**Location:** `transformed_data/artist_data/artist_20251018_120000.csv`

**Columns:**
- `artist_id` - Spotify artist ID (22 chars)
- `artist_name` - Artist name
- `artist_url` - Spotify URL
- `extracted_at` - Timestamp when extracted from API
- `transformed_at` - Timestamp when transformed to CSV

### Songs CSV
**Location:** `transformed_data/song_data/song_20251018_120000.csv`

**Columns:**
- `song_id` - Spotify track ID
- `song_name` - Track title
- `duration_ms` - Duration in milliseconds
- `url` - Spotify track URL
- `popularity` - Popularity score (0-100)
- `song_added` - When track was added to playlist
- `album_id` - Foreign key to albums
- `artist_id` - Foreign key to artists
- `extracted_at` - Timestamp when extracted from API
- `transformed_at` - Timestamp when transformed to CSV

## Prerequisites

- AWS S3 bucket: `spotify-etl-project-mang`
- IAM role with permissions:
  - `s3:GetObject` (read from raw_data/)
  - `s3:PutObject` (write to transformed_data/)
  - `s3:DeleteObject` (delete from to_processed/)
  - `s3:ListBucket`
  - `logs:CreateLogGroup`
  - `logs:CreateLogStream`
  - `logs:PutLogEvents`

## Setup Instructions

### Step 1: Prepare Lambda Code

Ensure you have the transformation Lambda code:
- File: `lambda/transformation/lambda_function.py`
- Code processes JSON → CSV with validation

### Step 2: Install Dependencies

```bash
cd lambda/transformation
pip install -r requirements.txt
```

### Step 3: Create Deployment Package

```bash
cd lambda/transformation

# Create package directory
mkdir package

# Install dependencies to package
pip install -r requirements.txt -t package/

# Copy Lambda function
cp lambda_function.py package/

# Create zip file
cd package
zip -r ../function.zip .
cd ..
```

### Step 4: Deploy to AWS Lambda

#### Via AWS Console

1. Go to AWS Lambda Console: https://console.aws.amazon.com/lambda
2. Click **"Create function"**
3. Configure:
   - **Function name:** `spotify-transformation`
   - **Runtime:** Python 3.9
   - **Architecture:** x86_64
4. Click **"Create function"**
5. Upload deployment package:
   - **Code source** → **Upload from** → **.zip file**
   - Select `function.zip`
   - Click **"Save"**
6. Configure settings:
   - **General configuration** → **Edit**
   - **Timeout:** 5 minutes
   - **Memory:** 512 MB
7. No environment variables needed (bucket name hardcoded)

#### Via AWS CLI

```bash
aws lambda create-function \
  --function-name spotify-transformation \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://function.zip \
  --timeout 300 \
  --memory-size 512
```

### Step 5: Configure S3 Trigger

Set Lambda to trigger automatically when new JSON files arrive:

1. Go to Lambda function → **Configuration** → **Triggers**
2. Click **"Add trigger"**
3. Configure:
   - **Source:** S3
   - **Bucket:** `spotify-etl-project-mang`
   - **Event type:** All object create events
   - **Prefix:** `raw_data/to_processed/`
   - **Suffix:** `.json`
4. Click **"Add"**

Now Lambda will auto-execute when extraction Lambda saves JSON to S3!

### Step 6: Test Transformation

#### Manual Test via Console

1. Upload test JSON to S3:
   ```bash
   aws s3 cp test_playlist.json s3://spotify-etl-project-mang/raw_data/to_processed/
   ```

2. Lambda triggers automatically

3. Check CloudWatch Logs for execution

4. Verify output:
   ```bash
   aws s3 ls s3://spotify-etl-project-mang/transformed_data/album_data/
   aws s3 ls s3://spotify-etl-project-mang/transformed_data/artist_data/
   aws s3 ls s3://spotify-etl-project-mang/transformed_data/song_data/
   ```

5. Verify JSON moved:
   ```bash
   aws s3 ls s3://spotify-etl-project-mang/raw_data/already_processed/
   ```

## Monitoring

### View CloudWatch Logs

```bash
# Stream live logs
aws logs tail /aws/lambda/spotify-transformation --follow

# View recent logs
aws logs tail /aws/lambda/spotify-transformation --since 1h
```

### Key Log Messages

**Success indicators:**
```
Loaded: raw_data/to_processed/playlist_XXX.json with 50 tracks
Extracted at: 2025-10-18T12:00:00Z
Extracted: 12 albums, 15 artists, 50 songs
After deduplication: 10 albums, 12 artists, 50 songs
Transformation completed at: 2025-10-18T12:01:30Z
Saved: transformed_data/album_data/album_20251018_120130.csv
Saved: transformed_data/song_data/song_20251018_120130.csv
Saved: transformed_data/artist_data/artist_20251018_120130.csv
Moved: raw_data/to_processed/playlist_XXX.json → raw_data/already_processed/playlist_XXX.json
Transformation completed successfully
```

**Warning messages:**
```
WARNING: 5/50 songs missing URL
WARNING: No extracted_at timestamp found, using current time
WARNING: playlist_XXX.json missing 'tracks' field, skipping
```

### Check Output Files

```bash
# List transformed files
aws s3 ls s3://spotify-etl-project-mang/transformed_data/album_data/ --recursive
aws s3 ls s3://spotify-etl-project-mang/transformed_data/artist_data/ --recursive
aws s3 ls s3://spotify-etl-project-mang/transformed_data/song_data/ --recursive

# Download for inspection
aws s3 cp s3://spotify-etl-project-mang/transformed_data/song_data/song_latest.csv .
```

## Troubleshooting

### Error: "No files found in bucket"

**Cause:** No JSON files in `raw_data/to_processed/`

**Solution:**
1. Check extraction Lambda ran successfully
2. Verify files uploaded to correct S3 path:
   ```bash
   aws s3 ls s3://spotify-etl-project-mang/raw_data/to_processed/
   ```

### Error: "Access Denied" (S3)

**Cause:** Lambda execution role missing S3 permissions

**Solution:**
Add inline policy to Lambda execution role:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::spotify-etl-project-mang/*",
        "arn:aws:s3:::spotify-etl-project-mang"
      ]
    }
  ]
}
```

### Error: "Task timed out"

**Cause:** Processing large playlists (1000+ tracks)

**Solution:**
1. Increase Lambda timeout to 10 minutes
2. Increase memory to 1024 MB (faster processing)

### Error: "Module not found: pandas"

**Cause:** Deployment package missing dependencies

**Solution:**
Rebuild deployment package:
```bash
rm -rf package function.zip
mkdir package
pip install -r requirements.txt -t package/
cp lambda_function.py package/
cd package
zip -r ../function.zip .
cd ..
```

Upload new `function.zip` to Lambda.

### Warning: "Missing URL" or "Missing added_at"

**Cause:** Spotify API returned incomplete data

**Impact:** CSV will have NULL values in those columns

**Solution:** 
- This is logged but not fatal
- Snowflake will handle NULLs (configured in DDL)
- Monitor frequency in CloudWatch Logs

## Data Validation

### Deduplication Logic

```python
# Remove duplicate albums (same album_id)
album_df.drop_duplicates(subset=['album_id'])

# Remove duplicate artists (same artist_id)
artist_df.drop_duplicates(subset=['artist_id'])

# Remove duplicate songs (same song_id)
song_df.drop_duplicates(subset=['song_id'])
```

**Why?** Playlists can have multiple songs from same album/artist.

### NULL Handling

```python
# Convert dates safely (errors='coerce' converts invalid to NaT)
album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')
```

**Result:** Invalid dates become `NaT` (Not a Time) → exported as empty in CSV → Snowflake loads as NULL

### Field Validation

- ✅ Skips tracks without `song_id`
- ✅ Skips albums without `album_id`
- ✅ Skips artists without `artist_id`
- ✅ Logs warnings for missing optional fields (URL, added_at)

## Cost Estimate

| Service | Usage | Cost/Month |
|---------|-------|-----------|
| **Lambda Invocations** | 30 runs/month × 30 sec | $0.00 (Free tier) |
| **Lambda Compute** | 512 MB × 30 sec × 30 runs | $0.00 (Free tier) |
| **S3 Storage** | +300 MB CSV files | $0.007 |
| **S3 Requests** | 90 PUT requests | $0.00 |
| **Total** | | **~$0.007/month** |

## Performance Metrics

**Typical execution (50 tracks):**
- Duration: 15-30 seconds
- Memory used: 150-200 MB
- Billed duration: ~30 seconds

**Large playlist (500 tracks):**
- Duration: 60-90 seconds
- Memory used: 300-400 MB
- Billed duration: ~90 seconds

## Next Steps

After successful transformation:
1. CSV files trigger Snowpipe (via S3 event notification)
2. Snowpipe auto-loads CSV → Snowflake tables
3. Data available for analytics queries

## Pipeline Integration

```
┌──────────────┐
│  Extraction  │
│    Lambda    │
└──────┬───────┘
       │ Saves JSON
       ↓
┌──────────────┐
│   S3: raw/   │
│to_processed/ │
└──────┬───────┘
       │ Triggers
       ↓
┌──────────────┐
│Transformation│ ← YOU ARE HERE
│    Lambda    │
└──────┬───────┘
       │ Saves CSV
       ↓
┌──────────────┐
│S3:transformed│
│    _data/    │
└──────┬───────┘
       │ Triggers
       ↓
┌──────────────┐
│  Snowpipe    │
│  (Snowflake) │
└──────────────┘
```

## Support

**Issues?**
- Check CloudWatch Logs for detailed error messages
- Verify S3 bucket structure matches expected paths
- Ensure IAM role has all required permissions

**Documentation:**
- Pandas: https://pandas.pydata.org/docs/
- Boto3 S3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
- AWS Lambda: https://docs.aws.amazon.com/lambda/

---

**Last Updated:** 2025-10-18# Spotify Transformation Lambda

## Status: ��� In Development

AWS Lambda function for transforming raw JSON to CSV format.

### Features (Planned)
- Parse JSON playlist data
- Normalize into albums, artists, songs
- Output CSV files to S3
- Data validation and deduplication

### Setup
Coming soon...

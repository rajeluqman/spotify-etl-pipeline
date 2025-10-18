# Spotify Extraction Lambda

## Status: ✅ Production Ready

AWS Lambda function that extracts Spotify playlist data via Spotify Web API and saves raw JSON to S3.

## Features

- ✅ Authenticate with Spotify API using client credentials flow
- ✅ Extract playlist tracks with full metadata (song, artist, album, duration, popularity)
- ✅ Save raw JSON to S3 bucket (`raw_data/to_processed/`)
- ✅ Error handling with retry logic
- ✅ CloudWatch logging for monitoring
- ✅ Handles API rate limiting

## Tech Stack

- **Runtime:** Python 3.9
- **Libraries:** spotipy, boto3, python-dotenv
- **AWS Services:** Lambda, S3, EventBridge, CloudWatch

## Prerequisites

### 1. Spotify Developer Account

1. Go to: https://developer.spotify.com/dashboard
2. Log in with your Spotify account
3. Click **"Create app"**
4. Fill in:
   - **App name:** Spotify ETL Pipeline
   - **App description:** Data extraction for analytics
   - **Redirect URI:** http://localhost (not used, but required)
5. Check ✅ agreement boxes
6. Click **"Save"**
7. Copy your **Client ID** and **Client Secret**

### 2. AWS Account

- S3 bucket created: `spotify-etl-project-mang`
- IAM role with permissions:
  - `s3:PutObject`
  - `s3:GetObject`
  - `logs:CreateLogGroup`
  - `logs:CreateLogStream`
  - `logs:PutLogEvents`

### 3. Spotify Playlist ID

Get playlist ID from URL:
```
https://open.spotify.com/playlist/69kJ5kOz2mKHTdg9FLHVVF
                                   └─────────────┬─────────────┘
                                           Playlist ID
```

## Setup Instructions

### Step 1: Install Dependencies (Local Testing)

```bash
cd lambda/extraction
pip install -r requirements.txt
```

### Step 2: Configure Environment Variables

Create `.env` file in project root (NOT committed to Git):

```env
SPOTIFY_CLIENT_ID=your_spotify_client_id_here
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret_here
SPOTIFY_PLAYLIST_ID=69kJ5kOz2mKHTdg9FLHVVF
S3_BUCKET_NAME=spotify-etl-project-mang
AWS_REGION=us-east-1
```

### Step 3: Deploy to AWS Lambda

#### Create Deployment Package

```bash
cd lambda/extraction

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

#### Deploy via AWS Console

1. Go to AWS Lambda Console: https://console.aws.amazon.com/lambda
2. Click **"Create function"**
3. Configure:
   - **Function name:** `spotify-extraction`
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
7. Set environment variables:
   - **Configuration** → **Environment variables** → **Edit**
   - Add:
     - `SPOTIFY_CLIENT_ID` = (your client ID)
     - `SPOTIFY_CLIENT_SECRET` = (your client secret)
     - `SPOTIFY_PLAYLIST_ID` = `69kJ5kOz2mKHTdg9FLHVVF`
     - `S3_BUCKET_NAME` = `spotify-etl-project-mang`

### Step 4: Test Lambda Function

1. Go to Lambda function page
2. Click **"Test"** tab
3. Create test event:
   - **Event name:** `test-extraction`
   - **Event JSON:**
     ```json
     {
       "playlist_id": "69kJ5kOz2mKHTdg9FLHVVF"
     }
     ```
4. Click **"Test"**
5. Check execution result (should see success message)

### Step 5: Schedule Daily Execution (Optional)

Create EventBridge rule for automatic daily runs at 2 AM UTC:

1. Go to Amazon EventBridge Console
2. Click **"Create rule"**
3. Configure:
   - **Name:** `spotify-daily-extraction`
   - **Rule type:** Schedule
   - **Schedule pattern:** `cron(0 2 * * ? *)`
4. Select target:
   - **Target:** Lambda function
   - **Function:** `spotify-extraction`
5. Click **"Create"**

## Monitoring

### View CloudWatch Logs

```bash
# Stream live logs
aws logs tail /aws/lambda/spotify-extraction --follow

# View recent logs
aws logs tail /aws/lambda/spotify-extraction --since 1h
```

### Check S3 Output

```bash
# List files in S3
aws s3 ls s3://spotify-etl-project-mang/raw_data/to_processed/

# Download latest file
aws s3 cp s3://spotify-etl-project-mang/raw_data/to_processed/playlist_latest.json .
```

## Troubleshooting

### Error: "Invalid client credentials"

**Cause:** Wrong Spotify API credentials

**Solution:**
1. Verify `SPOTIFY_CLIENT_ID` in Lambda environment variables
2. Verify `SPOTIFY_CLIENT_SECRET` in Lambda environment variables
3. Check Spotify Developer Dashboard for correct credentials

### Error: "Access Denied" (S3)

**Cause:** Lambda execution role missing S3 permissions

**Solution:**
1. Go to IAM → Roles → (your Lambda execution role)
2. Add inline policy:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:PutObject",
           "s3:GetObject"
         ],
         "Resource": "arn:aws:s3:::spotify-etl-project-mang/*"
       }
     ]
   }
   ```

### Error: "Task timed out after 3.00 seconds"

**Cause:** Default Lambda timeout too short

**Solution:**
1. Go to Lambda → Configuration → General configuration → Edit
2. Set **Timeout:** 5 minutes (300 seconds)
3. Click **Save**

## Cost Estimate

| Service | Usage | Cost/Month |
|---------|-------|-----------|
| **Lambda** | 30 invocations/month × 30 sec | $0.00 (Free tier) |
| **S3 Storage** | ~100 MB | $0.002 |
| **CloudWatch Logs** | ~10 MB/month | $0.00 (Free tier) |
| **Total** | | **~$0.002/month** |

## Output Format

**File naming:** `playlist_{PLAYLIST_ID}_{TIMESTAMP}.json`

**Example:** `playlist_69kJ5kOz2mKHTdg9FLHVVF_20251018_120000.json`

**JSON structure:**
```json
{
  "playlist_id": "69kJ5kOz2mKHTdg9FLHVVF",
  "playlist_name": "Your Playlist Name",
  "extracted_at": "2025-10-18T12:00:00Z",
  "total_tracks": 50,
  "tracks": [
    {
      "track_id": "abc123",
      "track_name": "Song Name",
      "artist_id": "xyz789",
      "artist_name": "Artist Name",
      "album_id": "def456",
      "album_name": "Album Name",
      "duration_ms": 180000,
      "popularity": 85,
      "added_at": "2025-10-01T10:30:00Z"
    }
  ]
}
```

## Next Steps

After successful extraction:
1. **Transformation Lambda** processes JSON → CSV
2. CSV files saved to `transformed_data/` in S3
3. **Snowpipe** auto-loads CSV to Snowflake tables
4. Analytics queries available in Snowflake

## Support

**Issues?**
- Check CloudWatch Logs for error details
- Verify S3 bucket for file uploads
- Check Spotify API status: https://status.spotify.com

**Documentation:**
- Spotify Web API: https://developer.spotify.com/documentation/web-api
- AWS Lambda: https://docs.aws.amazon.com/lambda
- Spotipy Library: https://spotipy.readthedocs.io

---

**Last Updated:** 2025-10-18# Spotify Extraction Lambda

## Status: ��� In Development

AWS Lambda function for extracting Spotify playlist data via API.

### Features (Planned)
- Extract playlist metadata
- Save raw JSON to S3
- Error handling and logging
- Scheduled execution via EventBridge

### Setup
Coming soon...

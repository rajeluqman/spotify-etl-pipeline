# Snowflake Deployment Guide

Complete setup instructions for Spotify ETL pipeline Snowflake data warehouse.

## Prerequisites

- [ ] Snowflake account with ACCOUNTADMIN role
- [ ] AWS account with S3 and IAM access
- [ ] S3 bucket: `spotify-etl-project-mang`
- [ ] CSV files in S3: `transformed_data/album_data/`, `artist_data/`, `song_data/`

## Deployment Checklist

- [ ] Step 1: AWS IAM Role Setup
- [ ] Step 2: Snowflake Storage Integration
- [ ] Step 3: Run DDL Script
- [ ] Step 4: Configure S3 Event Notifications
- [ ] Step 5: Verify Data Loading
- [ ] Step 6: Monitor Snowpipes

---

## Step 1: AWS IAM Role Setup

### 1.1 Create IAM Role

1. Go to AWS Console → IAM → Roles
2. Click **Create role**
3. Select **AWS account** → **Another AWS account**
4. Enter Snowflake AWS Account ID (will get this in Step 2.3)
5. Check **Require external ID** (will get this in Step 2.3)
6. Click **Next**

### 1.2 Attach Permissions Policy

Create inline policy with S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::spotify-etl-project-mang",
        "arn:aws:s3:::spotify-etl-project-mang/transformed_data/*"
      ]
    }
  ]
}
```

### 1.3 Name the Role

- **Role name:** `spotify-snowflake-s3-connection`
- **Description:** `Snowflake access to Spotify S3 bucket`
- Click **Create role**

### 1.4 Copy Role ARN

Copy the role ARN for Step 2:
```
arn:aws:iam::588738604241:role/spotify-snowflake-s3-connection
```

---

## Step 2: Snowflake Storage Integration

### 2.1 Login to Snowflake

- Use ACCOUNTADMIN role
- Open SQL worksheet

### 2.2 Run Section 1 & 2 of DDL

```sql
-- From snowflake/ddl/spotify_snowflake_ddl.sql
-- Run SECTION 1: Database & Schema Setup
-- Run SECTION 2: Table Definitions
```

### 2.3 Create Storage Integration

```sql
-- From SECTION 3 of DDL
CREATE OR REPLACE STORAGE INTEGRATION s3_spotify_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::588738604241:role/spotify-snowflake-s3-connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-mang/transformed_data/');

DESC STORAGE INTEGRATION s3_spotify_integration;
```

### 2.4 Retrieve Snowflake AWS User ARN and External ID

From `DESC STORAGE INTEGRATION` output, copy:
- `STORAGE_AWS_IAM_USER_ARN` 
- `STORAGE_AWS_EXTERNAL_ID`

Example:
```
STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::123456789012:user/abc-s3-connection
STORAGE_AWS_EXTERNAL_ID: ABC12345_SFCRole=1_AbCdEfGhIjKlMnOpQrStUvWxYz=
```

### 2.5 Update AWS IAM Trust Policy

1. Go back to AWS IAM → Roles → `spotify-snowflake-s3-connection`
2. Click **Trust relationships** tab
3. Click **Edit trust policy**
4. Replace with:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:user/abc-s3-connection"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "ABC12345_SFCRole=1_AbCdEfGhIjKlMnOpQrStUvWxYz="
        }
      }
    }
  ]
}
```

**Replace:**
- `arn:aws:iam::123456789012:user/abc-s3-connection` with your `STORAGE_AWS_IAM_USER_ARN`
- `ABC12345_SFCRole=1_AbCdEfGhIjKlMnOpQrStUvWxYz=` with your `STORAGE_AWS_EXTERNAL_ID`

5. Click **Update policy**

---

## Step 3: Run DDL Script

### 3.1 Complete DDL Execution

In Snowflake SQL worksheet, run remaining sections:

```sql
-- SECTION 3: Storage Integration & External Stage (already done)
-- SECTION 4: File Format & Initial Data Load
-- SECTION 5: Snowpipe Configuration
```

### 3.2 Verify Tables

```sql
SHOW TABLES LIKE 'tbl%';

SELECT 'Albums' AS table_name, COUNT(*) AS row_count FROM tblAlbum
UNION ALL
SELECT 'Artists', COUNT(*) FROM tblArtist
UNION ALL  
SELECT 'Songs', COUNT(*) FROM tblSongs;
```

Expected: Row counts > 0 if CSV files exist in S3.

---

## Step 4: Configure S3 Event Notifications

### 4.1 Retrieve Snowpipe SQS Queue ARNs

In Snowflake:

```sql
DESC PIPE pipe_albums;
DESC PIPE pipe_artists;
DESC PIPE pipe_songs;
```

Copy `notification_channel` value from each (SQS queue ARN).

Example:
```
arn:aws:sqs:us-east-1:123456789012:sf-snowpipe-AIDACKCEVSQ6C2EXAMPLE-AbCdEfGhIjKlMnOpQrStUvWxYz
```

### 4.2 Configure S3 Event Notification for Albums

1. Go to AWS S3 Console
2. Open bucket: `spotify-etl-project-mang`
3. Go to **Properties** tab
4. Scroll to **Event notifications**
5. Click **Create event notification**
6. Configure:
   - **Event name:** `snowpipe-albums-trigger`
   - **Prefix:** `transformed_data/album_data/`
   - **Suffix:** `.csv`
   - **Event types:** ☑️ All object create events
   - **Destination:** SQS queue
   - **SQS queue:** Select "Enter SQS queue ARN"
   - **SQS queue ARN:** Paste `notification_channel` from `pipe_albums`
7. Click **Save changes**

### 4.3 Repeat for Artists

- **Event name:** `snowpipe-artists-trigger`
- **Prefix:** `transformed_data/artist_data/`
- **Suffix:** `.csv`
- **SQS queue ARN:** From `pipe_artists`

### 4.4 Repeat for Songs

- **Event name:** `snowpipe-songs-trigger`
- **Prefix:** `transformed_data/song_data/`
- **Suffix:** `.csv`
- **SQS queue ARN:** From `pipe_songs`

---

## Step 5: Verify Data Loading

### 5.1 Test Snowpipe

Upload test CSV to S3:

```bash
aws s3 cp test_album.csv s3://spotify-etl-project-mang/transformed_data/album_data/
```

Wait 1-2 minutes, then check Snowflake:

```sql
SELECT COUNT(*) FROM tblAlbum;

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLALBUM',
    START_TIME => DATEADD(minutes, -10, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

### 5.2 Check Snowpipe Status

```sql
SELECT SYSTEM$PIPE_STATUS('pipe_albums');
SELECT SYSTEM$PIPE_STATUS('pipe_artists');
SELECT SYSTEM$PIPE_STATUS('pipe_songs');
```

Expected: Status should show as running or paused.

---

## Step 6: Monitor Snowpipes

### 6.1 Daily Monitoring

Run queries from `snowflake/monitoring/validation_queries.sql`:

- Row counts
- Data quality checks
- Referential integrity
- Load history
- Pipeline performance

### 6.2 Set Up Alerts (Optional)

Create Snowflake tasks to run monitoring queries and send alerts:

```sql
-- Example: Daily freshness check
CREATE OR REPLACE TASK check_data_freshness
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 9 * * * UTC'
AS
SELECT 
    CASE 
        WHEN MAX(loaded_at) < DATEADD(hour, -2, CURRENT_TIMESTAMP()) 
        THEN 'ALERT: No data loaded in last 2 hours'
        ELSE 'OK'
    END AS status
FROM tblSongs;

ALTER TASK check_data_freshness RESUME;
```

---

## Troubleshooting

### Issue: "Access Denied" when listing S3 files

**Cause:** IAM role trust policy incorrect

**Solution:**
1. Verify trust policy has correct Snowflake IAM user ARN
2. Verify external ID matches exactly
3. Check IAM role permissions include `s3:ListBucket`

### Issue: Snowpipe not loading files

**Cause:** S3 event notification not configured

**Solution:**
1. Verify event notification exists in S3 bucket properties
2. Check prefix/suffix matches file paths exactly
3. Verify SQS queue ARN is correct
4. Check Snowpipe status: `SELECT SYSTEM$PIPE_STATUS('pipe_name')`

### Issue: "File not found" in COPY INTO

**Cause:** Files not in expected S3 path

**Solution:**
```bash
# List files in S3
aws s3 ls s3://spotify-etl-project-mang/transformed_data/album_data/ --recursive

# Verify stage can see files
LIST @s3_spotify_stage/album_data/;
```

### Issue: Load errors with NULL values

**Cause:** CSV has empty fields not handled

**Solution:** Already configured in `fmt_csv_spotify`:
- `NULL_IF = ('', 'NULL', 'null', 'NaN')`
- `EMPTY_FIELD_AS_NULL = TRUE`

Check error details:
```sql
SELECT 
    FILE_NAME,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SPOTIFY_DB.RAW_DATA.TBLSONGS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED';
```

---

## Cost Optimization

### Warehouse Auto-Suspend

```sql
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;  -- 1 minute
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = TRUE;
```

### Snowpipe Costs

- Charged per compute second used
- Billed separately from warehouse usage
- Approximately $0.06 per 1000 files loaded

### Storage Costs

- $23/TB/month for on-demand storage
- $40/TB/month for time travel beyond 1 day

Monitor costs:
```sql
-- Snowpipe compute usage
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    PIPE_NAME => 'SPOTIFY_DB.RAW_DATA.pipe_songs',
    START_TIME => DATEADD(days, -30, CURRENT_TIMESTAMP())
));
```

---

## Next Steps

After successful deployment:

1. Schedule Lambda extraction to run daily
2. Configure monitoring dashboards
3. Set up email alerts for failures
4. Create analytics views for business users
5. Document data dictionary

---

## Support

**Issues?**
- Check CloudWatch Logs for Lambda errors
- Check Snowflake Query History for SQL errors
- Verify S3 event notifications are active
- Review monitoring queries for data quality issues

**Documentation:**
- Snowflake Storage Integration: https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
- Snowpipe: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- AWS IAM Trust Policies: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html

"""
AWS Lambda Function: Spotify Data Transformation - FINAL VERSION
FIXES:
- album_url (not 'url')
- artist_url from external_urls.spotify (not 'href')
- Added album_type and label fields
- Validation and error logging
- Move files to already_processed after completion
"""

import json
import boto3
import pandas as pd
from datetime import datetime 
from io import StringIO


def album(data, extracted_at):
    """
    Extract album data from Spotify playlist response.
    
    FIXED:
    - Column name: album_url (match Snowflake)
    - Added: album_type (single/album/compilation)
    - Added: label (record label)
    - Added: extracted_at (pass through from extract stage)
    """
    album_list = []
    missing_fields_log = []
    
    for row in data['tracks']:
        track = row.get('track')
        if not track or not track.get('album'):
            continue
            
        album_data = track['album']
        
        # Validate critical fields
        album_id = album_data.get('id')
        if not album_id:
            missing_fields_log.append("Missing album_id")
            continue
        
        # Extract all fields
        album_name = album_data.get('name')
        release_date = album_data.get('release_date')
        total_tracks = album_data.get('total_tracks')
        album_url = album_data.get('external_urls', {}).get('spotify')  # FIXED
        album_type = album_data.get('album_type')  # ADDED
        label = album_data.get('label')  # ADDED
        
        album_element = {
            'album_id': album_id,
            'album_name': album_name,
            'release_date': release_date,
            'total_tracks': total_tracks,
            'album_url': album_url,  # FIXED: renamed from 'url'
            'album_type': album_type,  # ADDED
            'label': label,  # ADDED
            'extracted_at': extracted_at  # ADDED: Pass through from extract stage
        }
        album_list.append(album_element)
    
    if missing_fields_log:
        print(f"Album extraction warnings: {len(missing_fields_log)} issues")
        print(f"Sample issues: {missing_fields_log[:5]}")
    
    return album_list


def artist(data, extracted_at):
    """
    Extract artist data from Spotify playlist response.
    
    FIXED:
    - artist_url from external_urls.spotify (not 'href')
    - Added: extracted_at (pass through from extract stage)
    """
    artist_list = []
    missing_fields_log = []
    
    for row in data['tracks']:
        track = row.get('track')
        if not track or not track.get('artists'):
            continue
            
        for artist in track['artists']:
            artist_id = artist.get('id')
            if not artist_id:
                missing_fields_log.append("Missing artist_id")
                continue
            
            artist_name = artist.get('name')
            artist_url = artist.get('external_urls', {}).get('spotify')  # FIXED
            
            artist_dict = {
                'artist_id': artist_id,
                'artist_name': artist_name,
                'artist_url': artist_url,  # FIXED: renamed from 'external_url'
                'extracted_at': extracted_at  # ADDED: Pass through from extract stage
            }
            artist_list.append(artist_dict)
    
    if missing_fields_log:
        print(f"Artist extraction warnings: {len(missing_fields_log)} issues")
    
    return artist_list


def song(data, extracted_at):
    """
    Extract song data from Spotify playlist response.
    
    FIXED:
    - url from track.external_urls.spotify
    - song_added from row.added_at
    - Added validation logging
    - Added: extracted_at (pass through from extract stage)
    """
    song_list = []
    missing_url_count = 0
    missing_added_at_count = 0
    
    for row in data['tracks']:
        track = row.get('track')
        if not track:
            continue

        song_id = track.get('id')
        if not song_id:
            continue
        
        song_name = track.get('name')
        duration_ms = track.get('duration_ms')
        popularity = track.get('popularity')
        
        # FIXED: Extract URL from track.external_urls.spotify
        url = track.get('external_urls', {}).get('spotify')
        if not url:
            missing_url_count += 1
        
        # FIXED: Extract added_at from row level
        song_added = row.get('added_at')
        if not song_added:
            missing_added_at_count += 1
        
        # Extract album_id and artist_id
        album_id = track.get('album', {}).get('id')
        artist_id = track.get('artists', [{}])[0].get('id') if track.get('artists') else None

        song_element = {
            'song_id': song_id,
            'song_name': song_name,
            'duration_ms': duration_ms,
            'url': url,  # FIXED
            'popularity': popularity,
            'song_added': song_added,  # FIXED
            'album_id': album_id,
            'artist_id': artist_id,
            'extracted_at': extracted_at  # ADDED: Pass through from extract stage
        }
        song_list.append(song_element)
    
    # Log warnings
    total_songs = len(song_list)
    if missing_url_count > 0:
        print(f"WARNING: {missing_url_count}/{total_songs} songs missing URL")
    if missing_added_at_count > 0:
        print(f"WARNING: {missing_added_at_count}/{total_songs} songs missing added_at")
    
    return song_list


def lambda_handler(event, context):
    """
    AWS Lambda handler for transformation.
    
    ADDED:
    - Better error handling
    - Validation before processing
    - Move files to already_processed after success
    """
    s3 = boto3.client('s3')
    bucket = 'spotify-etl-project-mang'
    key = 'raw_data/to_processed'

    spotify_data = []
    spotify_keys = []

    try:
        # List all JSON files in to_processed folder
        response = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        
        if 'Contents' not in response:
            print(f"No files found in {bucket}/{key}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No files to process'})
            }
        
        # Load all JSON files
        for file in response['Contents']:
            file_key = file['Key']
            if file_key.split('.')[-1] == 'json' and file_key != key:  # Skip folder itself
                try:
                    obj_response = s3.get_object(Bucket=bucket, Key=file_key)
                    content = obj_response['Body']
                    jsonObject = json.loads(content.read())
                    
                    # Validate JSON structure
                    if 'tracks' not in jsonObject:
                        print(f"WARNING: {file_key} missing 'tracks' field, skipping")
                        continue
                    
                    print(f"Loaded: {file_key} with {len(jsonObject.get('tracks', []))} tracks")
                    spotify_data.append(jsonObject)
                    spotify_keys.append(file_key)
                    
                except Exception as e:
                    print(f"ERROR loading {file_key}: {e}")
                    continue
        
        if not spotify_data:
            print("No valid JSON files to process")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No valid files to process'})
            }
        
        # Process each JSON file
        for idx, data in enumerate(spotify_data):
            print(f"\n=== Processing file {idx+1}/{len(spotify_data)} ===")
            
            # Get extracted_at from JSON (pass through from extract stage)
            extracted_at = data.get('extracted_at', data.get('extraction_timestamp'))
            if not extracted_at:
                print(f"WARNING: No extracted_at timestamp found, using current time")
                extracted_at = datetime.utcnow().isoformat()
            
            print(f"Extracted at: {extracted_at}")
            
            # Extract data (pass extracted_at to functions)
            album_list = album(data, extracted_at)
            artist_list = artist(data, extracted_at)
            song_list = song(data, extracted_at)
            
            print(f"Extracted: {len(album_list)} albums, {len(artist_list)} artists, {len(song_list)} songs")
            
            # Create DataFrames
            album_df = pd.DataFrame.from_dict(album_list).drop_duplicates(subset=['album_id'])
            artist_df = pd.DataFrame(artist_list).drop_duplicates(subset=['artist_id'])
            song_df = pd.DataFrame(song_list).drop_duplicates(subset=['song_id'])
            
            print(f"After deduplication: {len(album_df)} albums, {len(artist_df)} artists, {len(song_df)} songs")

            # ======================= Transformation =====================
            # Convert datetime columns safely
            album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
            song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')
            album_df['extracted_at'] = pd.to_datetime(album_df['extracted_at'], errors='coerce')
            artist_df['extracted_at'] = pd.to_datetime(artist_df['extracted_at'], errors='coerce')
            song_df['extracted_at'] = pd.to_datetime(song_df['extracted_at'], errors='coerce')
            
            # Add transformed_at timestamp (current time)
            transformed_at = datetime.utcnow()
            album_df['transformed_at'] = transformed_at
            artist_df['transformed_at'] = transformed_at
            song_df['transformed_at'] = transformed_at
            
            print(f"Transformation completed at: {transformed_at.isoformat()}")
            
            # Timestamp untuk file naming
            ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

            # ======================= Save to S3 =====================
            # Save album
            album_key = f"transformed_data/album_data/album_{ts}.csv"
            album_buffer = StringIO()
            album_df.to_csv(album_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=album_key, Body=album_buffer.getvalue())
            print(f"Saved: {album_key}")

            # Save song
            song_key = f"transformed_data/song_data/song_{ts}.csv"
            song_buffer = StringIO()
            song_df.to_csv(song_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=song_key, Body=song_buffer.getvalue())
            print(f"Saved: {song_key}")

            # Save artist
            artist_key = f"transformed_data/artist_data/artist_{ts}.csv"
            artist_buffer = StringIO()
            artist_df.to_csv(artist_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=artist_key, Body=artist_buffer.getvalue())
            print(f"Saved: {artist_key}")
            
            # ======================= Move processed file =====================
            # ADDED: Move from to_processed → already_processed
            source_key = spotify_keys[idx]
            dest_key = source_key.replace('to_processed', 'already_processed')
            
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': source_key},
                Key=dest_key
            )
            s3.delete_object(Bucket=bucket, Key=source_key)
            print(f"Moved: {source_key} → {dest_key}")
        
        print(f"\n=== Transformation completed successfully ===")
        print(f"Processed {len(spotify_data)} files")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Transformation completed successfully',
                'files_processed': len(spotify_data),
                'albums_saved': album_key,
                'songs_saved': song_key,
                'artists_saved': artist_key
            })
        }
        
    except Exception as e:
        print(f"ERROR in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'TRANSFORMATION_ERROR',
                'message': str(e)
            })
        }

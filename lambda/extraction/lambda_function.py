"""
AWS Lambda Function: Spotify Playlist Data Extraction (FIXED)
CHANGES:
- Added 'added_at' field to track when song was added to playlist
- Added 'external_urls' to get Spotify track URL
- Added more album fields: album_type, label for richer data
- Added artist external_urls for artist Spotify links
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, Any
import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SpotifyETLError(Exception):
    """Custom exception for Spotify ETL operations"""
    pass


class SpotifyETL:
    """Spotify ETL processor for extracting playlist data."""
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, 
                 s3_bucket: str, request_id: str = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.s3_bucket = s3_bucket
        self.request_id = request_id
        self.access_token = None
        self.s3_client = None
        
    def _get_access_token(self) -> str:
        """Exchange refresh token for fresh access token."""
        try:
            url = 'https://accounts.spotify.com/api/token'
            
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }
            
            auth = (self.client_id, self.client_secret)
            
            response = requests.post(url, data=data, auth=auth, timeout=30)
            
            if response.status_code != 200:
                raise SpotifyETLError(f"Token refresh failed: {response.text}")
            
            token_info = response.json()
            self.access_token = token_info['access_token']
            
            logger.info("Successfully obtained access token")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Token refresh request failed: {e}")
            raise SpotifyETLError(f"Token refresh failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during token refresh: {e}")
            raise SpotifyETLError(f"Token refresh failed: {e}")
    
    def _initialize_s3_client(self) -> None:
        """Initialize AWS S3 client."""
        try:
            self.s3_client = boto3.client('s3')
            logger.info("S3 client initialized")
        except Exception as e:
            logger.error(f"S3 client initialization failed: {e}")
            raise SpotifyETLError(f"S3 initialization failed: {e}")
    
    def extract_playlist_id(self, playlist_link: str) -> str:
        """Extract playlist ID from Spotify URL."""
        try:
            if not playlist_link or not isinstance(playlist_link, str):
                raise ValueError("Invalid playlist link")
            
            if "spotify.com/playlist/" not in playlist_link:
                raise ValueError("Not a valid Spotify playlist URL")
            
            playlist_id = playlist_link.split("/")[-1].split("?")[0]
            
            if not playlist_id or len(playlist_id) != 22:
                raise ValueError("Invalid playlist ID format")
            
            logger.info(f"Extracted playlist ID: {playlist_id}")
            return playlist_id
            
        except Exception as e:
            logger.error(f"Failed to extract playlist ID: {e}")
            raise SpotifyETLError(f"Playlist ID extraction failed: {e}")
    
    def extract_playlist_data(self, playlist_id: str) -> Dict[str, Any]:
        """
        Extract all tracks from Spotify playlist with pagination.
        
        FIXED: Now includes:
        - added_at: When track was added to playlist
        - external_urls: Spotify URLs for tracks and artists
        - album.album_type: single/album/compilation
        - album.label: Record label
        """
        try:
            if not self.access_token:
                raise SpotifyETLError("Access token not available")
            
            headers = {
                'Authorization': f'Bearer {self.access_token}'
            }
            
            # Get playlist metadata
            playlist_url = f'https://api.spotify.com/v1/playlists/{playlist_id}'
            playlist_params = {
                'fields': 'name,description,owner,public,followers'
            }
            
            playlist_response = requests.get(
                playlist_url, 
                headers=headers, 
                params=playlist_params,
                timeout=30
            )
            
            if playlist_response.status_code != 200:
                raise SpotifyETLError(f"Playlist fetch failed: {playlist_response.text}")
            
            playlist_info = playlist_response.json()
            
            # Get all tracks with pagination
            all_tracks = []
            offset = 0
            limit = 100
            
            while True:
                tracks_url = f'https://api.spotify.com/v1/playlists/{playlist_id}/tracks'
                
                # FIXED: Complete field specification
                tracks_params = {
                    'limit': limit,
                    'offset': offset,
                    'fields': (
                        'items('
                            'added_at,'  # ADDED: Track when song added to playlist
                            'track('
                                'id,name,duration_ms,popularity,explicit,'
                                'external_urls,'  # ADDED: Spotify track URL
                                'album(id,name,release_date,total_tracks,album_type,label,external_urls),'  # ADDED: album_type, label
                                'artists(id,name,external_urls)'  # ADDED: external_urls for artists
                            ')'
                        '),'
                        'next,total'
                    ),
                    'market': 'MY'
                }
                
                tracks_response = requests.get(
                    tracks_url,
                    headers=headers,
                    params=tracks_params,
                    timeout=30
                )
                
                if tracks_response.status_code != 200:
                    raise SpotifyETLError(f"Tracks fetch failed: {tracks_response.text}")
                
                tracks_data = tracks_response.json()
                all_tracks.extend(tracks_data['items'])
                
                logger.info(f"Extracted {len(all_tracks)} tracks so far...")
                
                # Check if more pages exist
                if not tracks_data.get('next'):
                    break
                
                offset += limit
            
            # Combine playlist info and tracks
            playlist_data = {
                'playlist_info': playlist_info,
                'tracks': all_tracks,
                'total_tracks': len(all_tracks),
                'extracted_at': datetime.utcnow().isoformat(),  # ADDED: Extraction timestamp
                'extraction_timestamp': datetime.utcnow().isoformat(),  # Keep for backward compatibility
                'playlist_id': playlist_id
            }
            
            logger.info(f"Successfully extracted {len(all_tracks)} tracks with complete metadata")
            return playlist_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise SpotifyETLError(f"Playlist data extraction failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during data extraction: {e}")
            raise SpotifyETLError(f"Data extraction failed: {e}")
    
    def upload_to_s3(self, data: Dict[str, Any], s3_key: str) -> bool:
        """Upload JSON data to S3 with encryption."""
        try:
            if not self.s3_client:
                raise SpotifyETLError("S3 client not initialized")
            
            json_data = json.dumps(data, indent=2, default=str, ensure_ascii=False)
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Successfully uploaded data to s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"S3 upload failed with error code {error_code}: {e}")
            raise SpotifyETLError(f"S3 upload failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during S3 upload: {e}")
            raise SpotifyETLError(f"S3 upload failed: {e}")
    
    def generate_s3_path(self, playlist_id: str, data_stage: str = 'to_processed') -> str:
        """Generate S3 path based on data stage."""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"playlist_{playlist_id}_{timestamp}.json"
        
        path_mapping = {
            'to_processed': f"raw_data/to_processed/{filename}",
            'already_processed': f"raw_data/already_processed/{filename}",
            'failed_extraction': f"raw_data/failed_extraction/{filename}"
        }
        
        return path_mapping.get(data_stage, path_mapping['to_processed'])
    
    def save_error_log(self, error_info: Dict[str, Any], playlist_id: str) -> None:
        """Save error logs to S3 for debugging."""
        try:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            error_filename = f"error_{playlist_id}_{timestamp}.json"
            error_s3_key = f"logs/error_logs/{error_filename}"
            
            error_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'playlist_id': playlist_id,
                'error_details': error_info,
                'lambda_request_id': self.request_id or 'unknown'
            }
            
            self.upload_to_s3(error_data, error_s3_key)
            logger.info(f"Error log saved to: s3://{self.s3_bucket}/{error_s3_key}")
            
        except Exception as e:
            logger.error(f"Failed to save error log: {e}")
    
    def process_playlist(self, playlist_link: str, data_stage: str = 'to_processed') -> Dict[str, Any]:
        """Main processing method - orchestrates entire ETL flow."""
        playlist_id = None
        
        try:
            self._get_access_token()
            self._initialize_s3_client()
            
            playlist_id = self.extract_playlist_id(playlist_link)
            playlist_data = self.extract_playlist_data(playlist_id)
            
            s3_key = self.generate_s3_path(playlist_id, data_stage)
            self.upload_to_s3(playlist_data, s3_key)
            
            execution_log = {
                'timestamp': datetime.utcnow().isoformat(),
                'playlist_id': playlist_id,
                'playlist_link': playlist_link,
                'data_stage': data_stage,
                'total_tracks': playlist_data['total_tracks'],
                's3_location': f"s3://{self.s3_bucket}/{s3_key}",
                'status': 'success'
            }
            
            execution_s3_key = f"logs/execution_logs/success_{playlist_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            self.upload_to_s3(execution_log, execution_s3_key)
            
            return {
                'status': 'success',
                'playlist_id': playlist_id,
                'total_tracks': playlist_data['total_tracks'],
                's3_location': f"s3://{self.s3_bucket}/{s3_key}",
                'data_stage': data_stage,
                'extraction_timestamp': playlist_data['extraction_timestamp']
            }
            
        except Exception as e:
            if playlist_id:
                error_info = {
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'playlist_link': playlist_link,
                    'data_stage': data_stage
                }
                
                self.save_error_log(error_info, playlist_id)
                
                try:
                    failed_data = {
                        'playlist_link': playlist_link,
                        'error': error_info,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    failed_s3_key = self.generate_s3_path(playlist_id, 'failed_extraction')
                    self.upload_to_s3(failed_data, failed_s3_key)
                except:
                    pass
            
            raise


def validate_environment_variables() -> Dict[str, str]:
    """Validate required environment variables are set."""
    required_vars = {
        'SPOTIFY_CLIENT_ID': os.environ.get('SPOTIFY_CLIENT_ID'),
        'SPOTIFY_CLIENT_SECRET': os.environ.get('SPOTIFY_CLIENT_SECRET'),
        'SPOTIFY_REFRESH_TOKEN': os.environ.get('SPOTIFY_REFRESH_TOKEN'),
        'S3_BUCKET_NAME': os.environ.get('S3_BUCKET_NAME', 'spotify-etl-project-mang')
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        raise SpotifyETLError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    logger.info(f"Environment variables validated successfully")
    
    return required_vars


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler - entry point for Lambda execution."""
    
    try:
        logger.info(f"Lambda function started. Event: {json.dumps(event, default=str)}")
        
        env_vars = validate_environment_variables()
        
        playlist_link = event.get('playlist_link', "https://open.spotify.com/playlist/69kJ5kOz2mKHTdg9FLHVVF")
        data_stage = event.get('data_stage', 'to_processed')
        
        if not playlist_link:
            raise SpotifyETLError("No playlist link provided")
        
        logger.info(f"Processing playlist: {playlist_link}")
        logger.info(f"Data stage: {data_stage}")
        
        etl_processor = SpotifyETL(
            client_id=env_vars['SPOTIFY_CLIENT_ID'],
            client_secret=env_vars['SPOTIFY_CLIENT_SECRET'],
            refresh_token=env_vars['SPOTIFY_REFRESH_TOKEN'],
            s3_bucket=env_vars['S3_BUCKET_NAME'],
            request_id=context.aws_request_id if context else None
        )
        
        result = etl_processor.process_playlist(playlist_link, data_stage)
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Playlist processed successfully',
                'data': result
            }, default=str)
        }
        
        logger.info(f"Lambda execution completed successfully: {result}")
        return response
        
    except SpotifyETLError as e:
        logger.error(f"ETL Error: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'ETL_ERROR',
                'message': str(e)
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'INTERNAL_ERROR',
                'message': 'An unexpected error occurred'
            })
        }

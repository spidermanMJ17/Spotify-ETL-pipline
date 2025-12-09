import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
import urllib.parse
import requests

def lambda_handler(event, context):
    
    # Fetch Spotify API credentials from environment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    # Authenticate using Spotify's Client Credentials Flow
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Get access token to make Spotify API calls
    access_token = sp.auth_manager.get_access_token(as_dict=False)
    
    # Define and encode the search query for Spotify playlist
    search_query = "Top 50 - India"
    q = urllib.parse.quote(search_query)
    url = f"https://api.spotify.com/v1/search?q={q}&type=playlist"

    # Search for the playlist
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    playlist_data = response.json()

    # Extract playlist ID (from 6th result in the list)
    playlist_id = playlist_data['playlists']['items'][5]['id']
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    # Fetch the playlist's track data
    response = requests.get(playlist_url, headers=headers)
    track_data = response.json()

    # Get the top 50 songs from the playlist
    top_50_songs = track_data['items'][0:50]

    # Save the data to S3 in a JSON file
    client = boto3.client('s3')
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    client.put_object(
        Bucket="spotify-etl-project-sannu",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(top_50_songs)
    )

    # Trigger AWS Glue job to transform and process the data
    glue = boto3.client('glue')
    gluejobname = "spotify_transform_s3_load"

    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)
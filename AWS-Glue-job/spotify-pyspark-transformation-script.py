import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date, to_timestamp
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3

# Initialize Spark and Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read raw JSON data from S3
s3_path = "s3://spotify-etl-project-sannu/raw_data/to_process/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths":[s3_path]},
    format="json",
    format_options={"jsonPath": "$[*]"}
)
spotify_df = source_dyf.toDF()

# Extract and clean Album data
album_df = spotify_df.select(
    col("track.album.id").alias("album_id"),
    col("track.album.name").alias("album_name"),
    col("track.album.release_date").alias("album_release_date"),
    col("track.album.total_tracks").alias("album_total_tracks"),
    col("track.album.external_urls.spotify").alias("album_url"),
).drop_duplicates(["album_id"])

album_df = album_df.withColumn(
    "album_release_date",
    to_date("album_release_date", "yyyy-MM-dd")
)

# Extract and clean Artist data
artist_df = spotify_df.select(col("track.artists"))
artist_df = artist_df.withColumn("artist", explode("artists"))
artist_df = artist_df.select(
    col("artist.id").alias("artist_id"),
    col("artist.name").alias("artist_name"),
    col("artist.external_urls.spotify").alias("external_url"),
).drop_duplicates(["artist_id"])

# Extract and clean Songs data
songs_df = spotify_df.select(
    col("track.id").alias("song_id"),
    col("track.name").alias("song_name"),
    col("track.duration_ms").alias("duration_ms"),
    col("track.external_urls.spotify").alias("url"),
    col("track.popularity").alias("popularity"),
    col("added_at").alias("song_added"),
    col("track.album.id").alias("album_id"),
    col("track.artists")
).drop_duplicates(["song_id"])

# Normalize artist_id from nested list
songs_df = songs_df.withColumn("artists", explode("artists")).withColumn("artists", col("artists.id"))
songs_df = songs_df.withColumnRenamed("artists", "artist_id")

# Format song_added timestamp
songs_df = songs_df.withColumn(
    "song_added",
    to_timestamp("song_added", "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

# Function to write DataFrame to S3 in specified format
def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": f"s3://spotify-etl-project-sannu/transformed_data/{path_suffix}/"},
        format = format_type
    )

# Generate timestamp for unique folder naming
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")

# Write transformed data to S3
write_to_s3(album_df, f"album/album_transformed_{timestamp}", "csv")
write_to_s3(artist_df, f"artist/artist_transformed_{timestamp}", "csv")
write_to_s3(songs_df, f"songs/song_transformed_{timestamp}", "csv")

# Move processed files to 'processed' folder in S3 and delete originals
s3 = boto3.client('s3')
Bucket = "spotify-etl-project-sannu"
Key = "raw_data/to_process/"

# List all .json files to move
spotify_keys = []
for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
    file_key = file['Key']
    if file_key.split('.')[-1] == "json":
        spotify_keys.append(file_key)

# Copy and delete files
s3_resource = boto3.resource('s3')
for key in spotify_keys:
    copy_source = {
        'Bucket': Bucket,
        'Key': key
    }
    s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])    
    s3_resource.Object(Bucket, key).delete()

# Commit the Glue job
job.commit()
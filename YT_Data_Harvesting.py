import streamlit as st
import json5
import pymongo
import mysql.connector
import isodate
import pandas as pd
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

def youtube_api_connect(api_key):
  youtube = build("youtube", "v3", developerKey=api_key)
  return youtube

def get_channel_data(youtube, channel_id):
  try:
    channel_request = youtube.channels().list(
      part = "sinppet, contentDetails, Statistics",
      id = channel_id
    )
    channel_response = channel_request.execute()

    if "items" in channel_response and len (channel_response["items"]) > 0:
      channel_data = channel_response["items"][0]
      return channel_data

    else:
      return None
  except HttpError as e:
    print("An error occurred:", e)
    return None

def get_playlists_data(youtube, channel_id):
  try:
    playlists_request = youtube.playlists().list(
      part = "snippet",
      channelId = channel_id,
      maxResults = 50
    )
    playlists_response = playlists_request.execute()

    playlists = playlists_response.get("items", [])
    playlists_data = []
    for playlist in playlists:
      playlist_data = {
        "playlist_name": playlist["snippet"]["title"],
        "playlist_id": playlist["id"],
        "videos": []
      }

      
      videos_data = get_video_data(youtube, playlist["id"])
      playlist_data["videos"] = videos_data

      playlists_data.append(playlist_data)

    return playlists_data, len(playlists)
  except HttpError as e:
    print("An error occurred:", e)
    return [], 0


def get_video_data(youtube, playlist_id):
  try:
    playlist_items_request = youtube.playlistItems().list(
      part = "snippet",
      playlistId = playlist_id,
      maxResults = 50
    )

    playlist_items_response = playlist_items_request.execute()

    playlist_items = playlist_items_response.get("items", [])
    video_ids = [item["snippet"]["resourceId"]["videoId"] for item in playlist_items]

    videos_data = []
    video_data_map = {}

    for i in range(0, len(video_ids), 50):
      video_ids_batch = video_ids[i:i+50]
      video_request = youtube.videos().list(
        part="snippet,contentDetails,statistics,status",
        id=",".join(video_ids_batch)
      )
      video_response = video_request.execute()

      videos = video_response.get("items", [])
      for video in videos:
          video_id = video["id"]
          video_data = video_data_map.get(video_id)

          if video_data is None:
            tags = video["snippet"].get("tags", [])
            duration = isodate.parse_duration(video["contentDetails"]["duration"]).total_seconds()

            video_data = {
              "video_id": video_id,
              "video_title": video["snippet"]["title"],
              "video_description": video["snippet"]["description"],
              "tags": tags,
              "published_at": video["snippet"]["publishedAt"],
              "view_count": video["statistics"]["viewCount"],
              "like_count": video["statistics"]["likeCount"],
              "dislike_count": 0,
              "favorite_count": video["statistics"]["favoriteCount"],
              "comment_count": video["statistics"]["commentCount"],
              "duration": duration,
              "thumbnail": video["snippet"]["thumbnails"]["default"]["url"],
              "caption_status": "Available" if video["contentDetails"]["caption"] else "Not Available",
              "comments": get_comments_data(youtube, video_id)
            }

            video_data_map[video_id] = video_data

          videos_data.append(video_data)

    return videos_data
  except HttpError as e:
    print("An HTTP error occurred:", e)
    return []


def get_comments_data(youtube, video_id):
  try:
    comments_request = youtube.commentThreads().list(
      part="snippet",
      videoId=video_id,
      maxResults=100
    )
    comments_response = comments_request.execute()

    comments = []
    while "items" in comments_response:
      for item in comments_response["items"]:
        comment_data = {
          "comment_id": item["id"],
          "comment_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
          "comment_author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
          "comment_published_at": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
        }
        comments.append(comment_data)

      if "nextPageToken" in comments_response:
        comments_request = youtube.commentThreads().list(
          part="snippet",
          videoId=video_id,
          maxResults=100,
          pageToken=comments_response["nextPageToken"]
        )
        comments_response = comments_request.execute()
      else:
        break
    return comments
  except HttpError as e:
    print("An error occurred:", e)
    return []

def get_multiple_channel_data(channel_ids,apikey):
  youtube = youtube_api_connect(apikey)
  channel_id_list = channel_ids.split(",")
  all_data = []
  datas = []

  for channel_id in channel_id_list:
    channel_data = get_channel_data(youtube, channel_id.strip())
    if channel_data:
      playlist_data, playlist_count = get_playlists_data(youtube, channel_id.strip())

      data = {
          "Channel_name": channel_data["snippet"]["title"],
          "Channel_Id": channel_data["id"],
          "Subscription_count": channel_data["statistics"]["subscriberCount"],
          "Channel_views": channel_data["statistics"]["viewCount"],
          "Channel_description": channel_data["snippet"]["description"],
          "Playlist_count": playlist_count,
          "Playlists": playlist_data
        }
      c_data = data.copy()

      if "Playlists" in c_data:
        del c_data["Playlists"]

      datas.append(c_data)
      all_data.append({"channel":data})

  return datas,all_data



def store_data_mongo(alldata):
  mongourl = st.secrets["MONGOURL"]
  try:
    with pymongo.MongoClient(mongourl) as client:
      db = client['YoutubeHacks']
      collection = db['ChannelData']
      inserted_channel_ids = []

      for i in alldata:
        channel_id = i["channel"]["Channel_Id"]
        filter_query = {"channel.Channel_Id": channel_id}
        if collection.count_documents(filter_query) > 0:
          inserted_channel_ids.append(channel_id)
        else:
          insert_result = collection.insert_one(i)

          if insert_result.inserted_id:
            inserted_channel_ids.append(channel_id)
          else:
            st.write("Failed to insert data.")
            
      return inserted_channel_ids
  except pymongo.errors.PyMongoError as e:
    st.write("An error occurred while storing data in MongoDB:", str(e))
  return None



def sql_connect():
  sql_host = st.secrets["DB_HOST"]
  sql_user = st.secrets["DB_USER"]
  sql_password =st.secrets["DB_PASS"]
  dbname = st.secrets["DB_NAME"]
  sql_port = st.secrets["DB_PORT"]
  conn = mysql.connector.connect(
         host = sql_host,
         port=sql_port,
         user=sql_user,
         password=sql_password,
         database = dbname
    )
  return conn

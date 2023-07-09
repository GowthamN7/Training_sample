import streamlit as st
import json
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

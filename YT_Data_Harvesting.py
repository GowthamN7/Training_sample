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

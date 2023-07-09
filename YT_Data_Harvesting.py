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

# Importing all the required libraries

import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
from pprint import pprint
import psycopg2
import pandas as pd


# Connecting to Application Programming Interface Key (API Key) 

def connect_api():
    api_key = "AIzaSyCjyRwpXIDehrP27mMRkdIFrpYlnt0XTsU"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version, developerKey=api_key)
    return youtube
youtube = connect_api()
    
# Getting Channel Information

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        details = {
            'Channel_Name': i['snippet']['title'],
            'Channel_Id': i['id'],
            'Subscribers': i['statistics']['subscriberCount'],
            'Views': i['statistics']['viewCount'],
            'Total_Videos': i['statistics']['videoCount'],
            'Channel_Description': i['snippet']['description'],
            'Playlist_Id': i['contentDetails']['relatedPlaylists']['uploads']
        }
        return details
    
    
# Getting Video Ids

def get_video_ids(channel_id):
    video_ids = [] 
    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in response1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids


# Get Video Information

def get_video_info(video_ids):
    video_details_list = []   # Create a list to store video details

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            details = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Tags': item.get('snippet', {}).get('tags', []),  # Use get to handle missing tags
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],  # Use a specific thumbnail (e.g., default)
                'Description': item['snippet'].get('description', 'Not Available'),  # Use get to handle missing description
                'Published_Date': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'Views': item['statistics'].get('viewCount', 0),  # Use get to handle missing view count
                'Likes':item['statistics'].get('likeCount',0),                
                'Comments': item['statistics'].get('commentCount', 0),  # Use get to handle missing comments
                'Favorite_Count': item['statistics'].get('favoriteCount', 0),  # Use get to handle missing favoriteCount
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails'].get('captions', 'Not Available')  # Use get to handle missing captions
            }

            video_details_list.append(details)  

    return video_details_list  

# Get Comment Information

from googleapiclient.errors import HttpError
def get_comment_info(video_ids):
    Comment_Information = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response5 = request.execute()

            for item in response5["items"]:
                comment_information = {
                    'Comment_Id': item["snippet"]["topLevelComment"]["id"],
                    'Video_Id': item["snippet"]["videoId"],
                    'Comment_Text': item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    'Comment_Author': item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    'Comment_Published': item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                Comment_Information.append(comment_information)

        except HttpError as e:
            if e.resp.status == 403 and "commentsDisabled" in str(e):
                print(f"Comments disabled for video {video_id}. Skipping.")
            else:
                print(f"Error processing video {video_id}: {e}")

    # Return statement should be outside the for loop
    return Comment_Information

# MongoDB Connection
# Replace <password> with the actual password for the user
password = "8cSL00cwupIVlexa"

# Replace "cluster0" and other parts with your MongoDB Atlas cluster details
cluster_uri = (
    "mongodb+srv://sriramdharini:" +
    f"{password}@cluster0.k9rma9d.mongodb.net/test?retryWrites=true&w=majority"
)

# Create a MongoClient instance
client = MongoClient(cluster_uri)
db = client["youtube_data"]

# Getting Playlist Details

def get_playlist_info(channel_id):
    
    next_page_token = None
    playlist_details_list = []

    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get('items', []):
            details = {
                'Playlist_Id': item['id'],
                'Title': item['snippet']['title'],
                'Channel_Id': item['snippet']['channelId'],
                'Channel_Name': item['snippet']['channelTitle'],
                'Published_Date': item['snippet']['publishedAt'],
                'Video_Count': item['contentDetails']['itemCount'],
            }

            # Append the details to the list
            playlist_details_list.append(details)

        # Check if there are more playlists to retrieve
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return playlist_details_list


# Install and import MongoDB

from pymongo import MongoClient

# Replace <password> with the actual password for the user
password = "8cSL00cwupIVlexa"

# Replace "cluster0" and other parts with your MongoDB Atlas cluster details
cluster_uri = (
    "mongodb+srv://sriramdharini:" +
    f"{password}@cluster0.k9rma9d.mongodb.net/test?retryWrites=true&w=majority"
)

# Create a MongoClient instance
client = MongoClient(cluster_uri)

# Uploading data to MongoDB

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details, "playlist_information": pl_details,
                      "video_information": vi_details,
                      "comment_information": com_details})

    return "upload completed successfully"

api_key = "AIzaSyCjyRwpXIDehrP27mMRkdIFrpYlnt0XTsU"
channel_ids = ["UCYO_jab_esuFRV4b17AJtAw", "UCh9nVJoWXmFb7sLApWGcLPQ", "UCJihyK0A38SZ6SdJirEdIOw",
               "UCzL_0nIe8B4-7ShhVPfJkgw", "UCHbq_l1qnuomfJCYQTsWf_Q", "UCduIoIMfD8tT3KoU0-zBRgQ",
               "UCSNeZleDn9c74yQc-EKnVTA", "UCtYLUTtgS3k1Fg4y5tAhLbw", "UC0WFn9iVCx3fCa31rqAvlXA",
               "UCvjgXvBlbQiydffZU7m1_aw"]

for channel_id in channel_ids:
    channel_data = get_channel_info(channel_id)
    # Assuming get_channel_info is a function that retrieves channel details
    if channel_data:
        # Insert channel_data into your database or perform other actions
        print(f"Channel details for {channel_id}: {channel_data}")
    else:
        print(f"Failed to retrieve channel details for {channel_id}")
        
# Install and Import Postgresql

# Table creation for channels,playlists,videos,coments


def channels_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "postgres",
                            database = "youtube_data",
                            port = "5432")                        

    cursor = mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)

    mydb.commit()
    try:
        create_query = '''
                    CREATE TABLE IF NOT EXISTS channels (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(80) PRIMARY KEY,
                        Subscribers BIGINT,
                        Views BIGINT,
                        Total_Videos INT,
                        Channel_Description TEXT,
                        Playlist_Id VARCHAR(50)
                    )
                '''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")
    # Iterate over MongoDB data and insert into PostgreSQL table
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channels (
                Channel_Name,
                Channel_Id,
                Subscribers,
                Views,
                Total_Videos,
                Channel_Description,
                Playlist_Id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscribers'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print(f"Inserted data for Channel: {row['Channel_Name']}")
        except Exception as e:
            print(f"Error inserting data: {e}")
            # Rollback the transaction in case of an error
            mydb.rollback()



def playlists_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="postgres",
            database="youtube_data",
            port="5432"
            )
    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(
                        Playlist_Id varchar(100) primary key,
                        Title varchar(100),
                        Channel_Id varchar(100),
                        Channel_Name varchar(100),
                        Published_Date timestamp,
                        Video_Count int
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists Table already created")

    db = client["youtube_data"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        print(pl_data)
        if "playlist_information" in pl_data and isinstance(pl_data["playlist_information"], list):
            for playlist_info in pl_data["playlist_information"]:
                pl_list.append(playlist_info)
    df1 = pd.DataFrame(pl_list)


    for index, row in df1.iterrows():
        insert_query = '''INSERT into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                Published_Date,
                                                Video_Count)
                                VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (
            row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['Published_Date'],
            row['Video_Count']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            st.write("Playlists values are already inserted")

import logging

def video_table():
    try:
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            database="youtube_data",
            port="5432"
        )
        cursor = mydb.cursor()

        drop_query = "DROP TABLE IF EXISTS videos"
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''CREATE TABLE IF NOT EXISTS videos(
                            Channel_Name VARCHAR(150),
                            Channel_Id VARCHAR(100),
                            Video_Id VARCHAR(50) PRIMARY KEY, 
                            Title VARCHAR(150), 
                            Tags TEXT,
                            Thumbnail VARCHAR(225),
                            Description TEXT, 
                            Published_Date TIMESTAMP,
                            Duration INTERVAL, 
                            Views BIGINT, 
                            Likes BIGINT,
                            Comments INT,
                            Favorite_Count INT, 
                            Definition VARCHAR(10), 
                            Caption_Status VARCHAR(50) 
                        )'''


        
        cursor.execute(create_query)             
        mydb.commit()

        vi_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
        df2 = pd.DataFrame(vi_list)

        for index, row in df2.iterrows():
            insert_query = '''
                INSERT INTO videos (Channel_Name,
                    Channel_Id,
                    Video_Id, 
                    Title, 
                    Tags,
                    Thumbnail,
                    Description, 
                    Published_Date,
                    Duration, 
                    Views, 
                    Likes,
                    Comments,
                    Favorite_Count, 
                    Definition, 
                    Caption_Status 
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                pd.to_datetime(row['Published_Date']),  # Convert to datetime if not already
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
            )

            cursor.execute(insert_query, values)
            mydb.commit()

    except psycopg2.Error as e:
        logging.error(f"Error interacting with PostgreSQL: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if mydb:
            mydb.close()
            
def comments_table():
    
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="postgres",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Commentsp Table already created")

    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               st.write("This comments are already exist in comments table")  
            
            
# Tables creation            
            
def tables():
    channels_table()
    playlists_table()
    video_table()
    comments_table()
    return "Tables Created successfully"
    
def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df= st.dataframe(ch_list)
    return df

def show_playlists_table():
    db = client["youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df1 = st.dataframe(pl_list)
    return df1

def show_video_table():
    vi_list = []
    db = client["youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2

def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    return df3



# Streamlit

import streamlit as st

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILLS")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL | Dharini Sriram")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_video_table()
elif show_table == ":blue[comments]":
    show_comments_table()

#SQL connection

mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="postgres",
            database= "youtube_data",
            port = "5432"
            )
cursor = mydb.cursor()

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. Likes of all videos',
     '7. Views of each channel',
     '8. Videos published in the year 2022',
     '9. Average duration of all videos in each channel',
     '10. Videos with highest number of comments'))

if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. Likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. Views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. Videos published in the year 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. Average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. Videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

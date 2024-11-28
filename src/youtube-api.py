'''
Very easy to hit the API limit. Need to find a way to handle this.  
'''

from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
import sqlite3
from datetime import datetime

# Load environment variables
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv('YOUTUBE_API_KEY')

def init_database():
    conn = sqlite3.connect('youtube_stats.db')
    cursor = conn.cursor()
    
    # Create channels table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            name TEXT,
            url TEXT,
            last_updated TIMESTAMP
        )
    ''')
    
    # Create subscriber history table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscriber_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT,
            subscriber_count INTEGER,
            fetch_date TIMESTAMP,
            FOREIGN KEY (channel_id) REFERENCES channels (channel_id)
        )
    ''')
    
    # Create index for faster queries
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_channel_date 
        ON subscriber_history (channel_id, fetch_date)
    ''')
    
    conn.commit()
    return conn

def search_channels(youtube, query):
    channels = []
    try:
        # Search for channels with the given query
        request = youtube.search().list(
            part='snippet',
            q=query,
            type='channel',
            maxResults=50  # Maximum allowed by API
        )
        
        response = request.execute()
        
        # Get channel IDs from search results
        channel_ids = [item['snippet']['channelId'] for item in response['items']]
        
        # Get detailed channel information in batches
        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i:i+50]
            channel_response = youtube.channels().list(
                part='statistics,snippet',
                id=','.join(batch)
            ).execute()
            
            for channel in channel_response['items']:
                # Some channels might hide their subscriber count
                if 'subscriberCount' in channel['statistics']:
                    channels.append({
                        'channel_id': channel['id'],
                        'name': channel['snippet']['title'],
                        'subscribers': int(channel['statistics']['subscriberCount']),
                        'url': f"https://youtube.com/channel/{channel['id']}"
                    })
    
    except Exception as e:
        print(f"Error searching for '{query}': {str(e)}")
    
    return channels

def get_top_channels():
    # Initialize database
    conn = init_database()
    cursor = conn.cursor()
    current_time = datetime.now()
    
    # Create YouTube API client
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    try:
        all_channels = []
        
        # Search queries targeting high-subscriber channels
        search_queries = [
            # Music Channels and Record Labels
            "music",
            "vevo",
            "blackpink",
            "bts",
            "sony music",
            "universal music",
            "warner music",
            
            # Individual Content Creators
            "pewdiepie",
            "mrbeast",
            "markiplier",
            "dude perfect",
            "jeffree star",
            "ninja",
            "jacksepticeye",
            "ksi",
            
            # Kids Content
            "kids diana show",
            "like nastya",
            "cocomelon",
            "vlad and niki",
            "pinkfong",
            "ryan's world",
            
            # Indian Channels
            "tseries",
            "zee music",
            "colors tv",
            "set india",
            "goldmines",
            
            # Sports & Gaming
            "wwe",
            "fifa",
            "espn",
            "gaming",
            "minecraft",
            "fortnite",
            
            # Entertainment & Media
            "netflix",
            "disney",
            "paramount",
            "ellen",
            "jimmy fallon",
            "james corden",
            
            # Educational
            "ted",
            "national geographic",
            "discovery",
            "khan academy",
            "vsauce",
            
            # News & Information
            "cnn",
            "bbc",
            "fox news",
            "abc news",
            
            # Technology
            "tech",
            "apple",
            "samsung",
            "microsoft",
            
            # YouTube Official
            "youtube",
            "youtube music",
            "youtube gaming",
            "youtube kids",
            
            # Latin American
            "badabun",
            "fernanfloo",
            "luisito comunica",
            
            # Arabic
            "arabic top",
            "shahid",
            "rotana",
            
            # Brazilian
            "kondzilla",
            "canal kond",
            
            # Korean
            "hybe labels",
            "sm entertainment",
            "jyp entertainment"
        ]
        
        # Collect channels from all searches
        for query in search_queries:
            print(f"Searching for channels related to '{query}'...")
            channels = search_channels(youtube, query)
            all_channels.extend(channels)
        
        # Remove duplicates based on channel_id
        unique_channels = {channel['channel_id']: channel for channel in all_channels}.values()
        
        # Update database with new data
        for channel in unique_channels:
            # Update or insert channel info
            cursor.execute('''
                INSERT OR REPLACE INTO channels (channel_id, name, url, last_updated)
                VALUES (?, ?, ?, ?)
            ''', (
                channel['channel_id'],
                channel['name'],
                channel['url'],
                current_time
            ))
            
            # Add new subscriber count record
            cursor.execute('''
                INSERT INTO subscriber_history (channel_id, subscriber_count, fetch_date)
                VALUES (?, ?, ?)
            ''', (
                channel['channel_id'],
                channel['subscribers'],
                current_time
            ))
        
        conn.commit()
        
        # Get top 20 channels with their current and previous subscriber counts
        cursor.execute('''
            WITH RankedChannels AS (
                -- Get the latest subscriber count for each channel
                SELECT 
                    c.name,
                    c.url,
                    sh.subscriber_count as current_subscribers,
                    sh.fetch_date as current_date,
                    (
                        SELECT subscriber_count
                        FROM subscriber_history sh2
                        WHERE sh2.channel_id = c.channel_id
                        AND sh2.fetch_date < sh.fetch_date
                        ORDER BY fetch_date DESC
                        LIMIT 1
                    ) as previous_subscribers
                FROM channels c
                JOIN subscriber_history sh ON c.channel_id = sh.channel_id
                WHERE sh.fetch_date = (
                    SELECT MAX(fetch_date)
                    FROM subscriber_history sh2
                    WHERE sh2.channel_id = c.channel_id
                )
                ORDER BY sh.subscriber_count DESC
                LIMIT 20
            )
            SELECT *,
                CASE 
                    WHEN previous_subscribers IS NOT NULL 
                    THEN current_subscribers - previous_subscribers
                    ELSE 0
                END as subscriber_change
            FROM RankedChannels
        ''')
        
        results = cursor.fetchall()
        
        print("\nTop 20 Most Subscribed YouTube Channels:")
        print("-" * 50)
        for i, (name, url, current_subs, current_date, prev_subs, sub_change) in enumerate(results, 1):
            print(f"{i}. {name}")
            print(f"   Subscribers: {current_subs:,}")
            if prev_subs:
                change_str = f"+{sub_change:,}" if sub_change >= 0 else f"{sub_change:,}"
                print(f"   Change: {change_str} since last update")
            print(f"   URL: {url}")
            print()
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    if not API_KEY:
        print("Please set your YouTube API key in the .env file")
    else:
        get_top_channels()
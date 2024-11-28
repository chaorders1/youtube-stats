# YouTube Top Channels Tracker

This script fetches the top 10 most subscribed YouTube channels using the YouTube Data API and saves the results to a CSV file.

## Setup

1. Create a Google Cloud Project:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the YouTube Data API v3

2. Get API Credentials:
   - In the Google Cloud Console, go to APIs & Services > Credentials
   - Create a new API Key
   - Copy the API key

3. Install required packages:
   ```bash
   pip install google-api-python-client python-dotenv
   ```

4. Create a `.env` file in the project directory and add your API key:
   ```
   YOUTUBE_API_KEY=your_api_key_here
   ```

## Usage

Run the script:
```bash
python youtube-api.py
```

The script will:
1. Fetch data for the top 10 most subscribed YouTube channels
2. Save the results to a CSV file with timestamp (e.g., `top_youtube_channels_20240321_143022.csv`)
3. Display the results in the console

The CSV file includes the following information for each channel:
- Channel name
- Subscriber count
- Video count
- Total view count
- Channel URL

## Note

The YouTube API has daily quota limits. Each API request consumes quota points, so be mindful of your usage.

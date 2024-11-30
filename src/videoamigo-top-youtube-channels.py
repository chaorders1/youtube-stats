#!/usr/bin/env python3
"""
YouTube Channel Scraper for VideoAmigo

This script scrapes top YouTube channel data from VideoAmigo's website.
It collects information such as channel rank, handle, subscriber count,
and generates proper YouTube URLs for each channel.

Usage:
    python videoamigo-top-youtube-channels.py [--limit LIMIT] [--headless]

Arguments:
    --limit LIMIT    Maximum number of channels to scrape (default: 10000)
    --headless      Run Chrome in headless mode (default: True)

Output:
    Creates a CSV file 'videoamigo-top-youtube-channels.csv' with the scraped data.

Requirements:
    - Python 3.6+
    - selenium
    - pandas
    - Chrome/Chromium browser
"""

import argparse
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

def convert_number(number_str):
    try:
        # Remove commas and convert to integer
        return int(number_str.replace(',', ''))
    except:
        return 0

def generate_youtube_url(channel_handle):
    # Check if the handle looks like a channel ID (starts with UC and is 24 characters long)
    if channel_handle.startswith('UC') and len(channel_handle) == 24:
        return f"https://www.youtube.com/channel/{channel_handle}"
    # Otherwise, treat it as a regular handle
    return f"https://www.youtube.com/@{channel_handle}"

def get_youtube_handle(channel_id):
    # If it looks like a channel ID (longer than 20 chars), return as is
    if len(channel_id) > 20:
        return channel_id
    # Otherwise, clean up the handle
    return channel_id.strip().replace('@', '')

def scrape_page(driver, offset):
    try:
        # Wait for the table to load (max 20 seconds)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.adjust-inline-flex.padding-mix.lc-align-center[title]'))
        )
        
        # Additional wait to ensure all elements are loaded
        time.sleep(5)
        
        # Get all the elements
        ranks = driver.find_elements(By.CSS_SELECTOR, 'div.adjust-inline-flex.padding-mix.lc-align-center[title]')
        channel_handles = driver.find_elements(By.CSS_SELECTOR, 'a[title][target="_blank"]')
        subscribers = driver.find_elements(By.CSS_SELECTOR, 'span.add-color[title]')
        
        # Process the data
        page_data = []
        for i in range(min(len(ranks), len(channel_handles), len(subscribers))):
            channel_id = channel_handles[i].get_attribute('title')
            handle = get_youtube_handle(channel_id)
            
            row = {
                'Rank': int(ranks[i].get_attribute('title')),
                'Channel_Handle': handle,
                'Channel_ID': channel_id,
                'Subscribers': convert_number(subscribers[i].get_attribute('title')),
                'YouTube_Channel_URL': generate_youtube_url(handle)
            }
            page_data.append(row)
        
        return page_data
    except Exception as e:
        print(f"Error scraping page with offset {offset}: {str(e)}")
        return []

# Set up command line arguments
parser = argparse.ArgumentParser(description='Scrape top YouTube channels from VideoAmigo')
parser.add_argument('--limit', type=int, default=10000, help='Maximum number of channels to scrape')
parser.add_argument('--headless', action='store_true', default=True, help='Run Chrome in headless mode')
args = parser.parse_args()

print("Setting up Chrome options...")
chrome_options = Options()
chrome_options.add_argument('--headless')  # Run in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')

print("Starting Chrome WebDriver...")
driver = webdriver.Chrome(options=chrome_options)

try:
    all_data = []
    # Scrape from 0 to 10000 with steps of 500
    for offset in range(0, args.limit, 500):
        print(f"\nScraping page with offset {offset}...")
        url = f'https://stats.videoamigo.com/top-youtube-channels-new/o-overall/subs/{offset}/desc'
        driver.get(url)
        
        page_data = scrape_page(driver, offset)
        if page_data:
            all_data.extend(page_data)
            print(f"Successfully scraped {len(page_data)} channels from offset {offset}")
        else:
            print(f"No data found for offset {offset}")
            # Try one more time with additional wait
            time.sleep(10)
            page_data = scrape_page(driver, offset)
            if page_data:
                all_data.extend(page_data)
                print(f"Retry successful: scraped {len(page_data)} channels from offset {offset}")
    
    if all_data:
        # Create DataFrame with all data
        df = pd.DataFrame(all_data)
        
        # Export to CSV
        df.to_csv('videoamigo-top-youtube-channels.csv', index=False, encoding='utf-8')
        print(f"\nTotal channels scraped: {len(all_data)}")
        print("Data has been saved to videoamigo-top-youtube-channels.csv")
        
        # Display first few rows
        print("\nFirst few rows of the data:")
        print(df.head())
    else:
        print("\nNo data was collected. Please check if the website is accessible.")

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    print("Closing Chrome WebDriver...")
    driver.quit()
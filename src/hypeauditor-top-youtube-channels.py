#!/usr/bin/env python3
"""
YouTube Channel Data Scraper

This script scrapes top YouTube channel data from HypeAuditor.com, including:
- Channel names and display names
- Subscriber counts
- Average views, likes, and comments
- Country information

Usage:
    python hypeauditor-top-youtube-channels.py [number_of_pages]

Arguments:
    number_of_pages: Optional. Number of pages to scrape (default: 20)
"""

import sys
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd

def convert_number(number_str):
    try:
        if 'M' in number_str:
            return float(number_str.replace('M', '')) * 1e6
        elif 'K' in number_str:
            return float(number_str.replace('K', '')) * 1e3
        else:
            return float(number_str)
    except:
        return 0

def get_youtube_url(channel_name):
    # Remove any spaces and special characters, convert to lowercase
    channel_handle = channel_name.lower().strip()
    return f"https://www.youtube.com/@{channel_handle}"

def scrape_page(driver):
    # Wait for the table to load
    time.sleep(5)  # Give the page some time to load
    
    # Get all the data using Selenium's find_elements with updated selectors
    names = driver.find_elements(By.CSS_SELECTOR, '.contributor__name')
    name_contents = driver.find_elements(By.CSS_SELECTOR, '.contributor__name-content')
    titles = driver.find_elements(By.CSS_SELECTOR, '.contributor__title')
    subscribers = driver.find_elements(By.CSS_SELECTOR, '.row-cell.subscribers')
    countries = driver.find_elements(By.CSS_SELECTOR, '.row-cell.audience')
    views = driver.find_elements(By.CSS_SELECTOR, '.row-cell.avg-views')
    likes = driver.find_elements(By.CSS_SELECTOR, '.row-cell.avg-likes')
    comments = driver.find_elements(By.CSS_SELECTOR, '.row-cell.avg-comments')
    
    # Process the data
    page_data = []
    for i in range(min(len(names), len(name_contents), len(titles), len(subscribers), len(countries), len(views), len(likes), len(comments))):
        channel_name = names[i].text.strip()
        row = {
            'Channel_Name': channel_name,
            'Display_Name': name_contents[i].text.strip(),
            'Title': titles[i].text.strip(),
            'Subscribers': convert_number(subscribers[i].text.strip()),
            'Country': countries[i].text.strip() or 'Unknown',
            'Avg_Views': convert_number(views[i].text.strip()),
            'Avg_Likes': convert_number(likes[i].text.strip()),
            'Avg_Comments': convert_number(comments[i].text.strip()),
            'YouTube_Channel_URL': get_youtube_url(channel_name)
        }
        page_data.append(row)
    
    return page_data

print("Setting up Chrome options...")
chrome_options = Options()
chrome_options.add_argument('--headless')  # Run in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

print("Starting Chrome WebDriver...")
driver = webdriver.Chrome(options=chrome_options)

try:
    all_data = []
    total_pages = 20  # default value
    if len(sys.argv) > 1:
        try:
            total_pages = int(sys.argv[1])
        except ValueError:
            print("Error: Please provide a valid number of pages")
            sys.exit(1)
    
    for page in range(1, total_pages + 1):
        print(f"\nScraping page {page} of {total_pages}...")
        url = f'https://hypeauditor.com/top-youtube/?p={page}'
        driver.get(url)
        
        page_data = scrape_page(driver)
        all_data.extend(page_data)
        
        print(f"Successfully scraped {len(page_data)} channels from page {page}")
    
    # Create DataFrame with all data
    df = pd.DataFrame(all_data)
    
    # Export to CSV
    df.to_csv('hyperauditor-top-youtube-channels.csv', index=False, encoding='utf-8')
    print(f"\nTotal channels scraped: {len(all_data)}")
    print("Data has been saved to hyperauditor-top-youtube-channels.csv")
    
    # Display first few rows
    print("\nFirst few rows of the data:")
    print(df.head())

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    print("Closing Chrome WebDriver...")
    driver.quit()
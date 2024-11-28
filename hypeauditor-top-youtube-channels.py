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

print("Setting up Chrome options...")
chrome_options = Options()
chrome_options.add_argument('--headless')  # Run in headless mode
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

print("Starting Chrome WebDriver...")
driver = webdriver.Chrome(options=chrome_options)

try:
    print("Navigating to the webpage...")
    url = 'https://hypeauditor.com/top-youtube/'
    driver.get(url)
    
    # Wait for the table to load
    print("Waiting for content to load...")
    time.sleep(5)  # Give the page some time to load
    
    print("Finding table elements...")
    # Get all the data using Selenium's find_elements with updated selectors
    names = driver.find_elements(By.CSS_SELECTOR, '.contributor__name')
    name_contents = driver.find_elements(By.CSS_SELECTOR, '.contributor__name-content')
    titles = driver.find_elements(By.CSS_SELECTOR, '.contributor__title')
    subscribers = driver.find_elements(By.CSS_SELECTOR, '.row-cell.subscribers')
    countries = driver.find_elements(By.CSS_SELECTOR, '.row-cell.audience')
    views = driver.find_elements(By.CSS_SELECTOR, '.row-cell.avg-views')
    likes = driver.find_elements(By.CSS_SELECTOR, '.row-cell.avg-likes')
    comments = driver.find_elements(By.CSS_SELECTOR, '.row-cell.avg-comments')
    
    print("\nData found:")
    print(f"Names: {len(names)}")
    print(f"Name contents: {len(name_contents)}")
    print(f"Titles: {len(titles)}")
    print(f"Subscribers: {len(subscribers)}")
    print(f"Countries: {len(countries)}")
    print(f"Views: {len(views)}")
    print(f"Likes: {len(likes)}")
    print(f"Comments: {len(comments)}")
    
    # Process the data
    data = []
    for i in range(min(len(names), len(name_contents), len(titles), len(subscribers), len(countries), len(views), len(likes), len(comments))):
        row = {
            'Channel Name': names[i].text.strip(),
            'Display Name': name_contents[i].text.strip(),
            'Title': titles[i].text.strip(),
            'Subscribers': convert_number(subscribers[i].text.strip()),
            'Country': countries[i].text.strip() or 'Unknown',
            'Avg Views': convert_number(views[i].text.strip()),
            'Avg Likes': convert_number(likes[i].text.strip()),
            'Avg Comments': convert_number(comments[i].text.strip())
        }
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Export to CSV
    df.to_csv('hyperauditor-top-youtube-channels.csv', index=False, encoding='utf-8')
    print("\nData has been saved to hyperauditor-top-youtube-channels.csv")
    
    # Display first few rows
    print("\nFirst few rows of the data:")
    print(df.head())

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    print("Closing Chrome WebDriver...")
    driver.quit()
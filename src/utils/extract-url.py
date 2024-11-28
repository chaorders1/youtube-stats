import requests
from bs4 import BeautifulSoup
import csv
import re
from urllib.parse import urlparse
import os

def is_valid_url(url):
    """Check if the given string is a valid URL."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def extract_urls(content):
    """Extract URLs starting with the specified prefix from HTML content."""
    soup = BeautifulSoup(content, 'html.parser')
    prefix = "https://stats.videoamigo.com/top-youtube-channels-new/"
    urls = []
    
    # Find all elements with href attributes
    for link in soup.find_all(href=True):
        if link['href'].startswith(prefix):
            urls.append(link['href'])
    
    # Find URLs in the text content using regex
    text_content = soup.get_text()
    url_pattern = f"{prefix}[^\s\"'<>]+"
    urls.extend(re.findall(url_pattern, text_content))
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(urls))

def save_to_csv(urls, output_file="extracted_urls.csv"):
    """Save the extracted URLs to a CSV file."""
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['URL'])  # Header
        for url in urls:
            writer.writerow([url])

def main():
    input_source = input("Enter the path to HTML file or URL: ").strip()
    
    try:
        if os.path.isfile(input_source):
            # Read from local file
            with open(input_source, 'r', encoding='utf-8') as f:
                content = f.read()
        elif is_valid_url(input_source):
            # Fetch content from URL
            response = requests.get(input_source)
            response.raise_for_status()
            content = response.text
        else:
            raise ValueError("Invalid input. Please provide a valid file path or URL.")
        
        # Extract URLs
        urls = extract_urls(content)
        
        if not urls:
            print("No matching URLs found.")
            return
        
        # Save to CSV
        output_file = "extracted_urls.csv"
        save_to_csv(urls, output_file)
        print(f"Successfully extracted {len(urls)} URLs and saved to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
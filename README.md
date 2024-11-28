# 🎥 YouTube Stats Tracker

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A powerful Python-based tool for tracking and analyzing YouTube channel statistics. This project combines web scraping and the YouTube Data API to gather comprehensive statistics about top YouTube channels, storing the data in both CSV and SQLite formats for easy analysis.

## ✨ Key Features

- 🔍 Scrapes top YouTube channel data from HypeAuditor
- 📊 Tracks subscriber counts and channel growth over time
- 🔌 Integrates with YouTube Data API for accurate statistics
- 💾 Stores data in both CSV and SQLite formats
- 📈 Monitors subscriber changes and channel rankings
- 🌐 Supports global channels across different categories (Music, Gaming, Education, etc.)

## 🚀 Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/youtube-stats.git
cd youtube-stats
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your YouTube API key:
   - Get an API key from the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a `.env` file in the project root
   - Add your API key: `YOUTUBE_API_KEY=your_api_key_here`

## 🎮 Quick Start

1. Scrape top YouTube channels:
```bash
python src/hypeauditor-top-youtube-channels.py
```

2. Update channel statistics using YouTube API: (This is under development)
```bash
python src/youtube-api.py
```

3. Convert CSV data to SQLite (optional):
```bash
python src/utils/csv-to-sqlite.py data/hyperauditor-top-youtube-channels.csv
```

## 📚 Project Structure

```
youtube-stats/
├── src/
│   ├── youtube-api.py           # YouTube API integration
│   ├── hypeauditor-top-youtube-channels.py  # Web scraping script
│   └── utils/
│       ├── csv-to-sqlite.py     # CSV to SQLite converter
│       └── csv-column-integer.py # CSV column type converter
├── data/                        # Data storage directory
├── requirements.txt             # Project dependencies
└── .env                        # Environment variables
```

## 📋 Prerequisites

- Python 3.8 or higher
- Chrome WebDriver (for web scraping)
- YouTube Data API key
- Required Python packages (see requirements.txt)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Important Notes

- Be mindful of YouTube API quotas when running the scripts
- The web scraping script includes appropriate delays to avoid rate limiting
- Some channels may hide their subscriber counts, affecting data collection

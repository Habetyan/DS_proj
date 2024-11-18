EPL Data Scraper and Prediction Model

This project aims to scrape data from Understat, process it, and use the extracted statistics to build a predictive model for upcoming English Premier League (EPL) games

Features
    
    Web Scraping:
        Extract team statistics (e.g., xG, goals, shots) for various categories (e.g., open play, set pieces).
        Retrieve match results, including scores and xG for each game.
        Organize data into team-specific directories with CSV files.

    Data Organization:
        Each team has its own directory containing:
            Detailed statistics for different play categories (situation.csv, formation.csv, etc.).
            Match results (matches.csv).

    Prediction Model:
        Use the scraped data to predict the outcomes of future games, leveraging machine learning techniques.

.
├── understat_scraper.py    # Main script to scrape data from Understat
├── requirements.txt        # Python dependencies for the project
├── README.md               # Project documentation
├── data/
│   ├── Liverpool/
│   │   ├── situation.csv
│   │   ├── matches.csv
│   │   └── ...
│   ├── Arsenal/
│   │   ├── situation.csv
│   │   ├── matches.csv
│   │   └── ...
│   └── ...
└── models/
    └── predictive_model.ipynb  # Jupyter Notebook for building the prediction model


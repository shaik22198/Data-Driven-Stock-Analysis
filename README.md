# Nifty 50 Stock Performance Dashboard

An end-to-end data analysis project that processes historical Nifty 50 stock data (Oct 2023 – Nov 2024), stores it in a MySQL database, and visualizes market trends using an interactive Streamlit dashboard.

## 🚀 Features

* **ETL Pipeline:** Extracts data from multiple YAML files, transforms it using Pandas, and loads it into a MySQL database via SQLAlchemy.
* **Volatility Analysis:** Identifies the top 10 most volatile stocks using standard deviation of daily returns.
* **Performance Tracking:** Calculates cumulative returns to find the top 5 performing stocks over time.
* **Sector-wise Insights:** Merges stock data with sector mappings to compare average yearly returns across industries.
* **Correlation Mapping:** Generates a $50 \times 50$ heatmap to identify stocks that move in tandem.
* **Monthly Gainers & Losers:** Dynamic subplots showing the top 5 gainers and losers for every month in the dataset.

## 🛠️ Tech Stack

* **Language:** Python
* **Data Processing:** Pandas, NumPy, YAML
* **Database:** MySQL, SQLAlchemy
* **Visualization:** Plotly Express, Plotly Graph Objects, Seaborn
* **Web Framework:** Streamlit

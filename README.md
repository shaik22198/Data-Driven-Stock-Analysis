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

## ⚙️ Setup and Installation

### 1. Install Dependencies
Run the following command in your terminal to install the required Python libraries:
```bash
pip install pandas pyyaml numpy matplotlib seaborn mysql-connector-python streamlit plotly sqlalchemy pymysql
```
### **2. Database Configuration**

* **Host:** Ensure **MySQL** is running on `localhost:3306`.
* **Database:** Create a new database named `stock_analysis`.
* **Credentials:** Update the following variables in `Mini-Project-2.py` with your **MySQL** details:

```python
user = "shaik"
password = "Password@000"
```
## **📊 How to Run**

To launch the **interactive dashboard**, open your terminal or command prompt in the project folder and execute the following command:

```bash
streamlit run Mini-Project-2.py
```

## **📈 Dashboard Preview**

* **Volatility Bar Chart:** Visualizes **risk levels** and price fluctuations across different stock tickers.
* **Cumulative Return Trend:** A multi-line chart comparing the **investment growth** of top performers like **TRENT** and **BEL**.
* **Sector Performance:** Identifies which industries (e.g., **Telecom**) outperformed the broader market.
* **Correlation Heatmap:** A detailed matrix exploring the **statistical relationship** and price movements between different stocks.

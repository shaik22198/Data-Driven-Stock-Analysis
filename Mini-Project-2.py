import pandas as pd
import yaml
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector as db
import streamlit as st
import plotly.express as px
import plotly.io as pio
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import plotly.graph_objects as go
import plotly.subplots as sp


# ----------------------------------------- Data Loading -----------------------------------------
def ymal_df(year, start_date, end_date):

    df_list = []
    dates = pd.date_range(start = start_date, end = end_date)
    dates_str = dates.strftime('%Y-%m-%d')
    for date in dates_str:
        p = date + '_05-30-00.yaml'
        try:
            with open(r"C:\Users\Salman\Data science\Mini-project 2\\" + year + p, 'r') as file:
                data = yaml.safe_load(file)
            df_current = pd.DataFrame(data)
            df_list.append(df_current)
        except:
            continue
    
    month_df = pd.concat(df_list)
    return month_df
  
_2023_10 = ymal_df("2023-10\\", "2023-10-01", "2023-10-31")
_2023_11 = ymal_df("2023-11\\", "2023-11-01", "2023-11-30")
_2023_12 = ymal_df("2023-12\\", "2023-12-01", "2023-12-31")
_2024_01 = ymal_df("2024-01\\", "2024-01-01", "2024-01-31")
_2024_02 = ymal_df("2024-02\\", "2024-02-01", "2024-02-29")
_2024_03 = ymal_df("2024-03\\", "2024-03-01", "2024-03-31")
_2024_04 = ymal_df("2024-04\\", "2024-04-01", "2024-04-30")
_2024_05 = ymal_df("2024-05\\", "2024-05-01", "2024-05-31")
_2024_06 = ymal_df("2024-06\\", "2024-06-01", "2024-06-30")
_2024_07 = ymal_df("2024-07\\", "2024-07-01", "2024-07-31")
_2024_08 = ymal_df("2024-08\\", "2024-08-01", "2024-08-31")
_2024_09 = ymal_df("2024-09\\", "2024-09-01", "2024-09-30")
_2024_10 = ymal_df("2024-10\\", "2024-10-01", "2024-10-31")
_2024_11 = ymal_df("2024-11\\", "2024-11-01", "2024-11-30")

final_df_list = [_2023_10, _2023_11, _2023_12, _2024_01, _2024_02, _2024_03, _2024_04, _2024_05, _2024_06, _2024_07, _2024_08, _2024_09, _2024_10, _2024_11 ]
final_df = pd.concat(final_df_list, ignore_index=True)


# ----------------------------------------- 1. Volatility Analysis -----------------------------------------
final_df["daily_return"] = (final_df["close"] - final_df.groupby(final_df['Ticker'])["close"].shift(1)) / final_df.groupby(final_df['Ticker'])["close"].shift(1)
stock_daily_return_std = final_df.groupby("Ticker")["daily_return"].std().reset_index().rename(columns={"Ticker": "stock", "daily_return": "std"})
top_10_volatile_stock = stock_daily_return_std.sort_values(by="std", ascending=False).head(10)


# ------------------------ Code to create function to transfer Dataframe from VS code to MySQL ------------------------
user = "shaik"
password = "Password@000"
host = "localhost"
port = 3306
db = "stock_analysis"
autocommit = True
encoded_password = quote_plus(password)

engine = create_engine(
    f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{db}"
)

def to_sql(dataframe, table_name):
    try:
        dataframe.to_sql(
            name=table_name, 
            con=engine, 
            index=False, 
            if_exists='fail' 
        )
        print("First run: Table created and data uploaded successfully!")

    except ValueError:
        print("Table already exists. Skipping upload to keep original data.")

# -------------------------- function to transfer top_10_volatile_stock DataFrame to MySQL as Table --------------------------
to_sql(top_10_volatile_stock, 'top_10_volatile_stocks')


# ----------------------------------------- 2. Cumulative Return Over Time -----------------------------------------
final_df["cumulative_return"] = (final_df.groupby("Ticker")["daily_return"].transform(lambda x: (1 + x).cumprod() - 1))
stock_total_return = final_df.groupby("Ticker")["cumulative_return"].last().reset_index().rename(columns = {"Ticker": "Stock"})
top_5_performing_stocks = stock_total_return.sort_values(by="cumulative_return", ascending=False).head(5)

gp = final_df.groupby("Ticker")

TRENT_df = gp.get_group("TRENT")
BEL_df = gp.get_group("BEL")
M_M_df = gp.get_group("M&M")
BAJAJ_AUTO_df = gp.get_group("BAJAJ-AUTO")
BHARTIARTL_df = gp.get_group("BHARTIARTL")


# ------------ function to transfer TRENT_df, BEL_df, M_M_df, BAJAJ_AUTO_df & BHARTIARTL_df DataFrames to MySQL as Table ------------
to_sql(TRENT_df, 'trent_cumulative_return')
to_sql(BEL_df, 'bel_cumulative_return')
to_sql(M_M_df, 'm_m_cumulative_return')
to_sql(BAJAJ_AUTO_df, 'bajaj_auto_cumulative_return')
to_sql(BHARTIARTL_df, 'bhartiartl_cumulative_return')


# ----------------------------------------- 3. Sector-wise Performance -----------------------------------------
def sym(s):
    sy = s.split(": ")
    return sy[1]

sector_df = pd.read_csv(r"C:\Users\Salman\Data science\Mini-project 2\Sector_data - Sheet1.csv")
sector_df["Ticker"] = sector_df["Symbol"].apply(lambda x: sym(x))

sector_df_list = []
gp = sector_df.groupby('sector')
for sector, df in gp:
    sector_df_list.append(df[["sector", "Ticker"]])

final_sector_df = pd.concat(sector_df_list)
final_sector_df_f = pd.merge(final_sector_df, stock_total_return, left_on='Ticker', right_on='Stock', how='left')

final_sector_df_f.loc[final_sector_df_f["Ticker"] == "TATACONSUMER", "Stock"] = 'TATACONSUMER'
final_sector_df_f.loc[final_sector_df_f["Ticker"] == "TATACONSUMER", "cumulative_return"] = 0.097538


final_sector_df_f["sector_yearly_return"] = final_sector_df_f.groupby("sector")["cumulative_return"].transform("mean")
gpb = final_sector_df_f.groupby("sector")

rows = []
for sector, df in gpb:
    rows.append({
        'sector' : sector,
        'yearly_return' : df["sector_yearly_return"].iloc[0]
    })
sector_plot_df = pd.DataFrame(rows)

sector_plot_df.loc[sector_plot_df["sector"] == "TELECOM", "yearly_return"] = 0.695990

# -------------------------- function to transfer sector_plot_df DataFrame to MySQL as Table --------------------------
to_sql(sector_plot_df, 'sector_wise_performance')


# ----------------------------------------- 4. Stock Price Correlation -----------------------------------------
returns_pivot = final_df.pivot(
    index="date",
    columns="Ticker",
    values="daily_return"
)

corr_matrix = returns_pivot.corr()

corr_pairs = (
    corr_matrix
    .where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    .stack()
)

corr_pairs.index.names = ["Stock_1", "Stock_2"]
corr_pairs_df = corr_pairs.reset_index(name="Correlation")
top_10_corr = (
    corr_pairs_df
    .assign(abs_corr=lambda x: x["Correlation"].abs())
    .sort_values("abs_corr", ascending=False)
    .drop(columns="abs_corr")
    .head(10)
)

top_10_corr["Stock_Pair"] = (
    top_10_corr["Stock_1"] + " | " + top_10_corr["Stock_2"]
)


# -------------------------- function to transfer corr_matrix & top_10_corr DataFrames to MySQL as Table --------------------------
to_sql(corr_matrix, 'correlation_matrix_heatmap')
to_sql(top_10_corr, 'top_10_correlated_stocks')


# ----------------------------------------- 5. Top 5 Gainers and Losers (Month-wise) -----------------------------------------

final_df["date"] = pd.to_datetime(final_df["date"])
final_df["month"] = final_df["date"].dt.to_period("M")

monthly_returns = (
    final_df
    .sort_values(["Ticker", "date"])
    .groupby(["Ticker", "month"])["daily_return"]
    .apply(lambda x: (1 + x).prod() - 1)
    .reset_index(name="monthly_return")
)

top_gainers_losers = (
    monthly_returns
    .groupby("month", group_keys=False)
    .apply(
        lambda x: pd.concat([
            x.nlargest(5, "monthly_return"),
            x.nsmallest(5, "monthly_return")
        ])
    )
)
months = sorted(top_gainers_losers["month"].unique())

# -------------------------- function to transfer top_gainers_losers DataFrame to MySQL as Table --------------------------
to_sql(top_gainers_losers, 'top_5_gainers_losers')




# -------------------------- Visualization in streamlit --------------------------
st.markdown(
    """
    <h2 style="
        text-align: center;
        color: #2c3e50;
        font-weight: 700;
    ">
        Nifty 50 Stock Performance Dashboard
    </h2>
    <p style="
        text-align: center;
        color: #7f8c8d;
        font-size: 16px;
        margin-top: -10px;
    ">
        Data-Driven Analysis (Oct 2013 – Nov 2024)
    </p>
    """,
    unsafe_allow_html=True
)


# -------------------------- Function to withdraw table from MySQL to VS Code as DataFrame --------------------------
def from_sql(sql_table_name, engine):
    query = f"SELECT * FROM {sql_table_name}"
    return pd.read_sql(query, con=engine)

volatility_df = from_sql("top_10_volatile_stocks", engine)


# -------------------------- Bar Plot for Top 10 Most Volatile Stocks --------------------------
fig = px.bar(
    volatility_df,
    x="stock",
    y="std",
    title="Top 10 Most Volatile Stocks",
    labels={
        "stock": "Stock",
        "std": "Standard Deviation"
    }
)

fig.update_layout(
    margin=dict(l=25, r=25, t=80, b=50),
    title=dict(
        font=dict(color="black", size=18)
    ),
    font=dict(color="black"),

    # ---- X AXIS ----
    xaxis=dict(
        tickangle=45,
        tickfont=dict(color="black", size=12),
        title=dict(
            font=dict(color="black", size=14)
        ),
        ticks="outside",
        tickcolor="black",
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        automargin=True   
    ),

    # ---- Y AXIS ----
    yaxis=dict(
        tickfont=dict(color="black", size=12),
        title=dict(
            font=dict(color="black", size=14)
        ),
        ticks="outside",
        tickcolor="black",
        showgrid=False,
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,  
        automargin=True
        
    )
)

st.plotly_chart(fig, use_container_width=True)


# -------------------------- Function to withdraw table from MySQL to VS Code as DataFrame --------------------------
df_trent = from_sql("trent_cumulative_return", engine)
df_bel = from_sql("bel_cumulative_return", engine)
df_m_m = from_sql("m_m_cumulative_return", engine)
df_bajaj_auto = from_sql("bajaj_auto_cumulative_return", engine)
df_bhartiartl = from_sql("bhartiartl_cumulative_return", engine)


# -------------------------- Scatter Plot for Cumulative Return Over Time --------------------------
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df_trent["date"],
    y=df_trent["cumulative_return"],
    mode="lines",
    name="TRENT"
))

fig.add_trace(go.Scatter(
    x=df_bel["date"],
    y=df_bel["cumulative_return"],
    mode="lines",
    name="BEL"
))

fig.add_trace(go.Scatter(
    x=df_m_m["date"],
    y=df_m_m["cumulative_return"],
    mode="lines",
    name="M&M"
))

fig.add_trace(go.Scatter(
    x=df_bajaj_auto["date"],
    y=df_bajaj_auto["cumulative_return"],
    mode="lines",
    name="BAJAJ-AUTO"
))

fig.add_trace(go.Scatter(
    x=df_bhartiartl["date"],
    y=df_bhartiartl["cumulative_return"],
    mode="lines",
    name="BHARTIARTL"
))

fig.update_layout(
    title=dict(
        text="Cumulative Return Trend for Top 5 Performing Stocks",
        font=dict(color="black", size=18)
    ),
    xaxis_title="Date",
    yaxis_title="Cumulative Return",
    template="plotly_white",
    legend_title=dict(text="Stocks", font=dict(color="black")),
    hovermode="x unified",
    
    # ---- X AXIS ----
    xaxis=dict(
        showline=True,
        showgrid=False,
        linecolor="black",
        linewidth=1,
        mirror=True,          
        ticks="outside",       
        tickcolor="black",     
        tickfont=dict(color="black"),
        title=dict(font=dict(color="black"))
    ),

    # ---- Y AXIS ----
    yaxis=dict(
        showline=True,
        showgrid=False,
        linecolor="black",
        linewidth=1,
        mirror=True,           
        ticks="outside",       
        tickcolor="black",     
        tickfont=dict(color="black"),
        title=dict(font=dict(color="black"))
    ),
    
    # Adding margin ensures the box borders aren't clipped by the container
    margin=dict(l=25, r=25, t=80, b=50)
)

st.plotly_chart(fig, use_container_width=True)


# -------------------------- Function to withdraw table from MySQL to VS Code as DataFrame --------------------------
df_sector = from_sql("sector_wise_performance", engine)

# -------------------------- Bar Plot for Sector-wise Performance --------------------------
fig = px.bar(
    df_sector,
    x="sector",
    y="yearly_return",
    title="Sector-wise Performance",
    labels={
        "sector": "Sector",
        "yearly_return": "Average yearly return"
    }
)

fig.update_layout(
    margin=dict(l=25, r=25, t=80, b=50),
    title=dict(
        font=dict(color="black", size=18)
    ),
    font=dict(color="black"),

    # ---- X AXIS ----
    xaxis=dict(
        tickangle=45,
        tickfont=dict(color="black", size=12),
        title=dict(
            font=dict(color="black", size=14)
        ),
        ticks="outside",
        tickcolor="black",
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        automargin=True   
    ),

    # ---- Y AXIS ----
    yaxis=dict(
        tickfont=dict(color="black", size=12),
        title=dict(
            font=dict(color="black", size=14)
        ),
        ticks="outside",
        tickcolor="black",
        showgrid=False,
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,  
        automargin=True
        
    )
)

st.plotly_chart(fig, use_container_width=True)


# -------------------------- Function to withdraw table from MySQL to VS Code as DataFrame --------------------------
df_corr_matrix = from_sql("correlation_matrix_heatmap", engine)
df_correlation = from_sql("top_10_correlated_stocks", engine)


# -------------------------- Heatmap for Stock Return Correlation --------------------------
fig = px.imshow(
    df_corr_matrix,
    text_auto=False,             
    aspect="auto",
    color_continuous_scale="RdBu_r",  
    range_color=[-1, 1],         # Ensures 0 is the center point
    labels=dict(color="Correlation"),
    title="Stock Return Correlation Heatmap (50 Stocks)"
)

# Adjust layout to match your figsize/styling
fig.update_layout(
    width=900, 
    height=800,
    xaxis_nticks=50,
    yaxis_nticks=50
)

# Display in Streamlit
st.plotly_chart(fig, use_container_width=True)

# -------------------------- Bar Plot for Top 10 Highly Correlated Stocks  --------------------------
fig = px.bar(
    df_correlation,
    x="Stock_Pair",
    y="Correlation",
    title="Top 10 Highly Correlated Stocks",
    labels={
        "Stock_Pair": "Stock Pair",
        "Correlation": "Correlation coefficient"
    }
)

fig.update_layout(
    margin=dict(l=25, r=25, t=80, b=50),
    title=dict(
        font=dict(color="black", size=18)
    ),
    font=dict(color="black"),

    # ---- X AXIS ----
    xaxis=dict(
        tickangle=45,
        tickfont=dict(color="black", size=12),
        title=dict(
            font=dict(color="black", size=14)
        ),
        ticks="outside",
        tickcolor="black",
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        automargin=True   
    ),

    # ---- Y AXIS ----
    yaxis=dict(
        tickfont=dict(color="black", size=12),
        title=dict(
            font=dict(color="black", size=14)
        ),
        ticks="outside",
        tickcolor="black",
        showgrid=False,
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,  
        automargin=True
        
    )
)

st.plotly_chart(fig, use_container_width=True)


# -------------------------- Function to withdraw table from MySQL to VS Code as DataFrame --------------------------
top_gainers_losers_df = from_sql("top_5_gainers_losers", engine)


# -------------------------- Bar Plot for Top 5 Gainers and Losers (Month-wise) --------------------------
# Setup the grid (7 rows, 2 columns for 14 months)
fig = sp.make_subplots(
    rows=7, cols=2, 
    subplot_titles=[str(m) for m in months],
    horizontal_spacing=0.1,
    vertical_spacing=0.05
)

# Iterate through each month and add to the grid
for i, month in enumerate(months):
    row = (i // 2) + 1
    col = (i % 2) + 1
   
    month_str = str(month)
    month_data = top_gainers_losers_df[top_gainers_losers_df["month"].astype(str) == month_str].sort_values("monthly_return")
    
    # Define colors: Green for positive, Red for negative
    colors = ['#EF553B' if val < 0 else '#00CC96' for val in month_data["monthly_return"]]
    
    fig.add_trace(
        go.Bar(
            x=month_data["monthly_return"],
            y=month_data["Ticker"],
            orientation='h',
            marker_color=colors,
            name=str(month),
            showlegend=False
        ),
        row=row, col=col
    )

# Update layout for clarity and height
fig.update_layout(
    title_text="Top 5 Gainers and Losers by Month",
    height=2500,  # Tall enough to fit 7 rows comfortably
    width=1000,
    template="plotly_white",
    margin=dict(l=50, r=50, t=100, b=50)
)

# Ensure x-axis is formatted as percentage
fig.update_xaxes(tickformat=".1%")

# Display in Streamlit
st.plotly_chart(fig, use_container_width=True)




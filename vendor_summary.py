import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine
import urllib

# -------------------------
# Setup Logging
# -------------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# -------------------------
# SQL Server Configuration
# -------------------------
server = 'LAPTOP-MJSKGTL4\\SQLEXPRESS'
database = 'Inventory'
driver = 'ODBC+Driver+17+for+SQL+Server'

params = urllib.parse.quote_plus(
    f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# -------------------------
# Ingest Cleaned Data to New Table
# -------------------------
def ingest_db(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested data into: {table_name}")
    except Exception as e:
        logging.error(f"❌ Failed to ingest data: {str(e)}")


# -------------------------
# Clean and Enrich Data
# -------------------------
def clean_data(df):
    try:
        df['Volume'] = df['Volume'].astype('float64')
        df.fillna(0, inplace=True)
        df['VendorName'] = df['VendorName'].str.strip()
        df['Description'] = df['Description'].str.strip()

        df['GrossProfit'] = round(df['TotalDollars'] - df['TotalPurchaseDollars'], 2)
        df['ProfitMargin'] = round((df['GrossProfit'] / df['TotalDollars']) * 100, 2)
        df['StockTurnover'] = round(df['TotalSalesQuantity'] / df['TotalPurchaseQantity'], 2)
        df['SalesToPurchaseRatio'] = round(df['TotalDollars'] / df['TotalPurchaseDollars'], 2)

        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.fillna(0, inplace=True)

        logging.info("Data cleaned and enriched.")
        return df
    except Exception as e:
        logging.error(f"Error cleaning data: {str(e)}")
        return df


# -------------------------
# Main Process
# -------------------------
if __name__ == '__main__':
    try:
        logging.info("Process started: Vendor Summary Extraction")
        
        # Load data from existing vendor_sales_summary table
        df = pd.read_sql("SELECT * FROM vendor_sales_summary", engine)
        logging.info(f"Loaded {len(df)} rows from vendor_sales_summary")

        # Rename column typo if needed
        if 'FreightCOst' in df.columns:
            df.rename(columns={'FreightCOst': 'FreightCost'}, inplace=True)

        # Clean and calculate metrics
        df_clean = clean_data(df)

        # Ingest to new table
        ingest_db(df_clean, 'new_vendor_sales_summary', engine)


    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

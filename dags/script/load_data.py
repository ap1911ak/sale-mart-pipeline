import pandas as pd
import os
BASE_DATA_PATH = "/opt/airflow/data"

LANDING_DIR = f"{BASE_DATA_PATH}/Landing_zone"
RAW_DIR = f"{BASE_DATA_PATH}/Raw_zone"
SERVING_DIR = f"{BASE_DATA_PATH}/Serving_zone"

def load_data():
    source_dir = RAW_DIR
    dest_dir = SERVING_DIR
    files = [f for f in os.listdir(source_dir) if f.endswith('.csv') and f.startswith('cleaned_')]

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    sales = pd.read_csv(f'{source_dir}/cleaned_sales.csv')
    products = pd.read_csv(f'{source_dir}/cleaned_products.csv')
    stores = pd.read_csv(f'{source_dir}/cleaned_stores.csv')

    # Create master sales data by merging sales with products and stores
    master_table = sales.merge(products, on='product_id', how='left') \
                        .merge(stores, on='store_id', how='left') \

    # Calculate total revenue 
    master_table['total_revenue'] = master_table['quantity'] * master_table['price']                        

    # Add date-related features
    master_table['date'] = pd.to_datetime(master_table['date'])
    master_table['day_of_week'] = master_table['date'].dt.dayofweek
    master_table['day'] = master_table['date'].dt.day_name()
    master_table['is_weekend'] = master_table['date'].dt.dayofweek >= 5

    #call formatting function
    master_table = format_header(master_table)

    # Rename columns for clarity
    master_table = master_table.rename(columns={'Quantity': 'Qty'})
    print(master_table)

    master_table.to_csv(f'{dest_dir}/master_sales_data.csv', index=False)

# formatting header to title case anf replacing underscores with spaces
def format_header(df):
    df.columns = [col.replace('_',' ').title() for col in df.columns]
    return df

if __name__ == "__main__":
    load_data()
import pandas as pd
import os
import datetime

def clean_data():
    source_dir = 'Raw_zone'
    prod_path = f'{source_dir}/products.csv'
    cust_path = f'{source_dir}/customers.csv'
    sales_path = f'{source_dir}/sales.csv'
    invent_path = f'{source_dir}/inventory.csv'
    promot_path = f'{source_dir}/promotions.csv'
    store_path = f'{source_dir}/stores.csv'

    clean_prod_path = f'{source_dir}/cleaned_products.csv'
    clean_cust_path = f'{source_dir}/cleaned_customers.csv'
    clean_sales_path = f'{source_dir}/cleaned_sales.csv'
    clean_invent_path = f'{source_dir}/cleaned_inventory.csv'
    clean_promot_path = f'{source_dir}/cleaned_promotions.csv'
    clean_store_path = f'{source_dir}/cleaned_stores.csv'

    # fill missing values in products.csv with 'AVG VALUE'
    df_prod = pd.read_csv(prod_path)
    df_prod['price'].fillna(df_prod.groupby('category')['price'].transform('mean'), inplace=True)
    print(df_prod.groupby('category')['price'].transform('mean'))

    # fill missing values in customers.csv with 'Unknown'
    df_cust = pd.read_csv(cust_path)
    df_cust['customer_name'].fillna('Unknown', inplace=True)

    # fill missing values in sales.csv with 1
    df_sales = pd.read_csv(sales_path)
    df_sales['quantity'].fillna(1, inplace=True)


    df_invent = pd.read_csv(invent_path)
    df_promot = pd.read_csv(promot_path)
    df_store = pd.read_csv(store_path)

    # Save cleaned data back to Raw_zone
    df_prod.to_csv(clean_prod_path, index=False)
    df_cust.to_csv(clean_cust_path, index=False)
    df_sales.to_csv(clean_sales_path, index=False)
    df_invent.to_csv(clean_invent_path, index=False)
    df_promot.to_csv(clean_promot_path, index=False)
    df_store.to_csv(clean_store_path, index=False)
    
if __name__ == "__main__":
    clean_data()
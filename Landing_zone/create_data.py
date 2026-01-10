import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 1. Products Table
products = pd.DataFrame({
    'product_id': range(101, 111),
    'product_name': ['Milk', 'Bread', 'Eggs', 'Rice', 'Chicken', 'Apple', 'Soap', 'Shampoo', 'Coke', 'Snack'],
    'category': ['Dairy', 'Bakery', 'Dairy', 'Grain', 'Meat', 'Fruit', 'Beauty', 'Beauty', 'Drink', 'Snack'],
    'price': [45.0, 30.0, 95.0, 150.0, 120.0, 25.0, 15.0, 89.0, 20.0, 10.0]
})
products.loc[2, 'price'] = np.nan # Missing price

# 2. Stores Table
stores = pd.DataFrame({
    'store_id': [1, 2, 3],
    'store_name': ['Bangkok Central', 'Chiang Mai North', 'Phuket South'],
    'region': ['Central', 'North', 'South']
})

# 3. Customers Table
customers = pd.DataFrame({
    'customer_id': range(501, 511),
    'customer_name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack'],
    'membership': ['Gold', 'Silver', 'Gold', 'Bronze', 'Silver', 'Bronze', 'Gold', 'Silver', 'Bronze', 'Gold']
})
customers.loc[4, 'customer_name'] = np.nan # Missing name

# 4. Sales Table (Fact Table) - 35 items
sales_data = []
for i in range(1, 36):
    sales_data.append({
        'transaction_id': f'T{1000+i}',
        'date': (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 10))).strftime('%Y-%m-%d'),
        'product_id': random.choice(range(101, 111)),
        'store_id': random.choice([1, 2, 3]),
        'customer_id': random.choice(range(501, 511)),
        'quantity': random.randint(1, 5)
    })
sales = pd.DataFrame(sales_data)
sales.loc[10, 'quantity'] = np.nan # Missing quantity

# 5. Promotions Table
promotions = pd.DataFrame({
    'promo_id': ['P01', 'P02', 'P03'],
    'promo_name': ['New Year Sale', 'Buy 1 Get 1', 'Member Discount'],
    'discount_pct': [0.1, 0.5, 0.15]
})

# 6. Inventory Table
inventory = pd.DataFrame({
    'product_id': range(101, 111),
    'stock_level': [100, 50, 20, 200, 40, 150, 80, 60, 300, 500]
})

# Exporting (Simulated)
files = {
    'products.csv': products,
    'stores.csv': stores,
    'customers.csv': customers,
    'sales.csv': sales,
    'promotions.csv': promotions,
    'inventory.csv': inventory
}

for name, df in files.items():
    df.to_csv(name, index=False)
    print(f"Created {name}")
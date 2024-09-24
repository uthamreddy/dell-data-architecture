import pandas as pd
import random
import numpy as np

# Define categories and products for sample data
categories = ['Laptops', 'Desktops', 'Monitors', 'Accessories']
products = {
    'Laptops': ['XPS 13', 'XPS 15', 'Latitude 5420', 'Inspiron 14'],
    'Desktops': ['Inspiron Desktop', 'Vostro Desktop', 'Alienware Aurora'],
    'Monitors': ['Dell 24 Monitor', 'Dell 27 Monitor', 'Alienware 34 Curved Monitor'],
    'Accessories': ['Dell Docking Station', 'Dell Wireless Mouse', 'Dell Keyboard']
}

# Generate sample data
data = []
for i in range(100):
    category = random.choice(categories)
    product = random.choice(products[category])
    order_id = random.randint(1000, 9999)
    units_sold = random.randint(1, 10)
    unit_price = round(random.uniform(50, 2000), 2)
    sales_date = np.random.choice(pd.date_range('2023-01-01', '2023-12-31'))
    
    data.append({
        'order_id': order_id,
        'product': product,
        'category': category,
        'units_sold': units_sold,
        'unit_price': unit_price,
        'sales_date': sales_date
    })

# Convert to DataFrame
df = pd.DataFrame(data)

# Save as CSV
csv_file_path = './sales-data/dell_sales_data.csv'
df.to_csv(csv_file_path, index=False)

csv_file_path

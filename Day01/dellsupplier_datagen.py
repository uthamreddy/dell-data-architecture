from faker import Faker
import pandas as pd
import random

fake = Faker()

# Function to generate Dell supplier data
def generate_supplier_data(num_suppliers=100):
    suppliers = []
    
    # Example product categories and their respective products
    product_categories = {
        "Laptops": ["Dell XPS 13", "Dell Inspiron 15", "Dell Latitude 7400"],
        "Monitors": ["Dell UltraSharp U2720Q", "Dell P2419H", "Dell SE2419HX"],
        "Servers": ["Dell PowerEdge R740", "Dell PowerEdge T340", "Dell PowerEdge R540"],
        "Storage": ["Dell EMC Unity XT", "Dell PowerVault ME4", "Dell Compellent SC5020"]
    }

    for _ in range(num_suppliers):
        # Choose a random product category and product
        category = random.choice(list(product_categories.keys()))
        product = random.choice(product_categories[category])

        supplier = {
            "supplier_name": fake.company(),
            "address": fake.address(),
            "email": fake.company_email(),
            "phone_number": fake.phone_number(),
            "product_category": category,
            "product_name": product,
            "supplier_quantity":random.randint(100,10000),
            "supplier_rating":random.randint(1, 10), #random supplier rating
            "delivery_time_in_days": random.randint(5, 30)  # Random delivery time
        }

        suppliers.append(supplier)
    
    return suppliers

# Generate and print supplier data
suppliers_data = generate_supplier_data(100)
"""for supplier in suppliers_data:
    print(supplier)"""

dfSuppliers = pd.DataFrame(suppliers_data)
print("Total records and columns",dfSuppliers.shape)
from IPython.display import display
display(dfSuppliers)
dfSuppliers.to_csv("/home/labuser/Desktop/Persistent_Folder/Utham/Day01/dellsuppliers_synthetic.csv",index=False,header=True)

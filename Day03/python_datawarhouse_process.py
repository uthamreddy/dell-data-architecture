import pandas as pd
from IPython.display import display
from sqlalchemy import create_engine
import time

csv_file_path = '/home/labuser/Desktop/Persistent_Folder/Utham/Day03/Superstore.csv'


def main(filename):
    pd.options.display.max_rows = 2000
    sales_data = pd.read_csv(csv_file_path,header=0,index_col=0, sep=',')
    #display(sales_data)
    #print(sales_data.shape[0])
    #print(sales_data.describe())
    #print(pd.options.display.max_rows) 
    performDataAccumlation(sales_data)

def performDataAccumlation(sales_data):
    column_name ="Sales"
    total_sales = sales_data[column_name].sum()
    average_sales = sales_data[column_name].mean()
    sales_by_category = sales_data.groupby('Category')[column_name].sum()
    sales_by_segment = sales_data.groupby('Segment')[column_name].sum()
    print(f"Total Sales: {total_sales}")
    print("X-------------------X------------------------X")
    print(f"Average Sales: {average_sales}")
    print("X-------------------X------------------------X")
    print(sales_by_category)
    print(sales_by_segment)
  
    # calculate time taken by the process
    start_time = time.time()
    sinkDatatoWarehouse(sales_data)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

def sinkDatatoWarehouse(sales_data):
    
    try:

        db_user = 'neondb_owner'
        db_password = '5xIPFpD8Aawi'
        db_host = 'ep-winter-wildflower-a54fxdv2.us-east-2.aws.neon.tech'
        db_port = '5432'
        db_name = 'dell_superstore'

        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        sales_data.to_sql('sales_data_superstore', engine, if_exists='replace', index=False)

        print("Data inserted into PostgreSQL successfully.")
  
    except Exception as ex:
        print(ex)



if __name__ == "__main__":
    main(csv_file_path)

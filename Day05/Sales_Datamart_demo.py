from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, count, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DellSalesDataMart") \
    .master("local[*]")\
    .getOrCreate()

try:

    # Load Dell sales data from a CSV
    
    sales_data_path = "./sales-data/dell_sales_data.csv" 



    sales_df = spark.read.csv(sales_data_path, header=True, inferSchema=True)


    # Calculate Total Sales Revenue for each product
    sales_df = sales_df.withColumn("total_revenue", col("units_sold") * col("unit_price"))

    # 1. Total Sales Revenue for all products
    total_sales_revenue = sales_df.agg(sum("total_revenue").alias("total_revenue")).collect()[0]['total_revenue']

    # 2. Average Order Value (AOV)
    aov = sales_df.groupBy("order_id").agg(sum("total_revenue").alias("order_revenue"))
    avg_order_value = aov.agg(avg("order_revenue").alias("avg_order_value")).collect()[0]['avg_order_value']

    # 3. Units Sold by Product Category
    units_sold_by_category = sales_df.groupBy("category").agg(sum("units_sold").alias("total_units_sold"))

    # 4. Top Selling Products by Revenue
    top_selling_products = sales_df.groupBy("product").agg(sum("total_revenue").alias("revenue")) \
        .orderBy(desc("revenue")).limit(10)

    # Display the results
    units_sold_by_category.show(truncate=False)
    top_selling_products.show(truncate=False)

    # Create Data Mart
    # Save the transformed data (Data Mart) into Parquet for reporting purposes

    # Save to Parquet
    output_path = "/datamart-sales/output/dell_sales_data_mart"
    top_selling_products.write.mode('overwrite').parquet(output_path)

    # Print the KPIs
    print(f"Total Sales Revenue: {total_sales_revenue}")
    print(f"Average Order Value (AOV): {avg_order_value}")

    sales_df_pd= units_sold_by_category.toPandas()

    from matplotlib import pyplot as plt
    colors = ['blue', 'green', 'red','orange'] 

    catagories = sales_df_pd["category"]
    
    explode = (0.05, 0.05, 0.05, 0.05) 
    sales_df_pd.plot(kind='pie', y='total_units_sold', autopct='%1.0f%%', colors=colors, explode=explode)


    plt.legend(catagories,
            title="Category",
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1))


    plt.title("Dell Sales By Category")

    #sales_df_pd.groupby(['category']).agg(sum("units_sold")).plot(kind='bar',x='Category',y='UnitsSold')
    plt.savefig("SalesData.pdf", format="pdf", bbox_inches="tight")
    plt.show()



except Exception as ex:
    print(ex)
finally:
    # Stop the Spark session
    spark.stop()

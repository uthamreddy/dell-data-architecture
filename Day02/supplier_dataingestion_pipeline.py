from pyspark.sql import *
from pyspark.sql.functions import *

try:
    # Initialize Spark session with Iceberg configurations
    spark = SparkSession.builder \
    .appName("dell-demo-scm") \
    .master("local[*]")\
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.postgresql:postgresql:42.2.23,org.apache.iceberg:iceberg-aws-bundle:1.5.2')\
    .config('spark.sql.catalog.dell_scm_catalog','org.apache.iceberg.spark.SparkCatalog')\
    .config("spark.sql.catalog.dell_scm_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")\
    .config("spark.sql.catalog.dell_scm_catalog.uri", "jdbc:postgresql://ep-winter-wildflower-a54fxdv2.us-east-2.aws.neon.tech/neondb?sslmode=require")\
    .config("spark.sql.catalog.dell_scm_catalog.verifyServerCertificate", "true")\
    .config("spark.sql.catalog.dell_scm_catalog.useSSL", "true")\
    .config("spark.sql.catalog.dell_scm_catalog.jdbc.user", "neondb_owner")\
    .config("spark.sql.catalog.dell_scm_catalog.jdbc.password", "5xIPFpD8Aawi")\
    .config("spark.sql.catalog.dell_scm_catalog.jdbc.driver", "org.postgresql.Driver")\
    .config("spark.sql.catalog.dell_scm_catalog.s3.endpoint", "http://172.17.0.1:9000")\
    .config("spark.sql.catalog.dell_scm_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")\
    .config("spark.sql.catalog.dell_scm_catalog.warehouse", "s3a://demo-dell-scm")\
    .config('spark.hadoop.fs.s3a.access.key', "SyVBytJJRPoVjhicfA41")\
    .config('spark.hadoop.fs.s3a.endpoint.region','us-east-1')\
    .config("spark.hadoop.fs.s3a.secret.key", "A3jVjMoXinC4bDfQW8TFZsAyyzZKASPApGgnYSoK")\
    .config("spark.sql.catalog.dell_scm_catalog.s3a.path-style-access", "true")\
    .config("spark.sql.catalogImplementation","in-memory")\
    .config("spark.executor.heartbeatInterval", "300000")\
    .config("spark.network.timeout", "400000")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.attempts.maximum", "1")\
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")\
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000")\
    .getOrCreate()

    supplier_df = spark.read.csv("/home/labuser/Desktop/Persistent_Folder/Utham/Day02/dellsuppliers_synthetic.csv", header=True, inferSchema=True)
  
    # Clean and Massage Data
    
    # Rename columns
    cols_list_rename = {'SupplierName': 'supplier_name', 'ProductCategory': 'product_category','Product':'product_name','SupplierRating':'supplier_rating','SupplierQuantity':'supplier_quantity'}

    for key, value in cols_list_rename.items():
        supplier_df= supplier_df.withColumnRenamed(value,key)

    # Drop rows with missing values in important columns
    cleaned_df = supplier_df.dropna(subset=["SupplierName", "ProductCategory", "Product", "SupplierRating"])

    # Transformation: Normalize supplier ratings (scale 1-5)
    transformed_df = cleaned_df.withColumn(
        "NormalizedRating", 
        when(col("SupplierRating") > 5, 5).otherwise(col("SupplierRating"))
    )

    # Group by Product Category and aggregate supplier details (Business Logic)
    # Average rating for each product category
    aggregated_df = transformed_df.groupBy("ProductCategory") \
        .agg(avg("NormalizedRating").alias("AvgSupplierRating"), 
            sum("SupplierQuantity").alias("TotalSuppliedQuantity"))

    # Write the DataFrame to an Iceberg table

    aggregated_df.writeTo("dell_scm_catalog.supplier_category").partitionedBy("ProductCategory").createOrReplace()

except Exception as ex:
    print(ex)

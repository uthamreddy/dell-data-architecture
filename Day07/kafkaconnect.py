from pyspark.sql import SparkSession
from pyspark.sql.functions import upper,explode,split

# Create a Spark Session
spark = SparkSession.builder \
    .appName("delldemo-kafkaapp") \
    .master("local[*]")\
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 2)\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

#Read the Stream from Kafka server
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Dellkafkademo")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
 #   .option("partition", 1)
    .load()
    .selectExpr("CAST(value AS STRING) as message")
)

transformMsgdf=df.withColumn("NewMessage",upper(df.message))

transformed_list = transformMsgdf.select(
    explode(
        split(transformMsgdf.NewMessage, " ")
    ).alias("Message")
    )


msg_count = transformed_list.groupBy("Message").count()


# run the query that prints the products and its count to console

query = msg_count \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

query.awaitTermination()

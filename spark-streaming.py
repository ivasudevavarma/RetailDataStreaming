# Retail Data Analysis
# ETL Pipe line to read Kafka datastream, process, calculate KPI's save a KPI data to the Json file for further processing. 
# Pre Defined Kafka server Details:
#                                       Bootstrap Server IP : 18.211.252.152
#                                       Port Number         : 9092
#                                       Topic Name          : real-time-project
#  
# Author : Vasudeva Varma Indukuri
# @Email : ivasudevavarma@gmail.com

# importing System dependencies
import os
import sys

#Setting up enviroment
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

# importing required pyspark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from builtins import round

# Initialize spark session
spark = SparkSession\
    .builder \
    .appName("OrderIntelligenceSystem") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Server and topic Details
serverDetails = "18.211.252.152:9092"
topicName = "real-time-project"

# Defining Schema 
schema = StructType() \
    .add("invoice_no",LongType()) \
    .add("country",StringType()) \
    .add("timestamp",TimestampType()) \
    .add("type",StringType()) \
    .add("items",ArrayType(
        StructType() \
            .add("SKU", StringType()) \
            .add("title", StringType()) \
            .add("unit_price", StringType()) \
            .add("quantity", StringType())
        )
    )

#Read Input from Kafka
orderRead = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",serverDetails) \
    .option("startingOffsets","Latest") \
    .option("subscribe",topicName) \
    .load() 

#Reading json data based on created schema
orderStream = orderRead.select(from_json(col("value").cast("string"),schema).alias("data")).select("data.*")

# Method total_Invoice_amount_method
# Description : The Component is used to find the total amount per invoice.
# Returns : Total sum of the amount of all items "+" float value for order and "-" for return.
def total_Invoice_amount_method(items,typeOfInvoice):
    amount = 0.0
    for item in items:
        totalPrice = (float(item['unit_price']) * int(item['quantity']))
        amount = amount + totalPrice
    if typeOfInvoice == "RETURN":
        amount = amount * -1
    amount = round(amount,2)
    return amount

# Method total_items_per_invoice_method
# Description : The Component is used to find the total no of items per invoice.
# Returns : Total sum of the amount of all items "+" float value for order and "-" for return.
def total_items_per_invoice_method(items):
    count = 0
    for item in items:
        count = count + int(item['quantity'])
    return count

# Defining User Defined function total_Invoice_amount with the utility functions
total_Invoice_amount = udf(total_Invoice_amount_method,DoubleType())

# Defining User Defined function total_items_per_invoice with the utility functions
total_items_per_invoice = udf(total_items_per_invoice_method,IntegerType())

# Adding all required columns to the DataFrame for further processing.
orderData = orderStream \
    .withColumn("Total_Amount",total_Invoice_amount(orderStream.items,orderStream.type)) \
    .withColumn("Total_Quantity",total_items_per_invoice(orderStream.items)) \
    .withColumn("Is_Order",when(col("type") == 'ORDER',1).otherwise(0)) \
    .withColumn("Is_Return",when(col("type") == 'RETURN',1).otherwise(0))

# Displaying summarized data on console.
ConsoleData = orderData \
    .select("invoice_no","country","timestamp","Total_Amount","Total_Quantity","Is_Order","Is_Return") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate","false") \
    .trigger(processingTime="1 minute") \
    .start()

# Calculating Time Based KPI's
ByTimeData = orderData \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute","1 minute")) \
    .agg(sum("Total_Amount").alias("Total volume of sales"),
    count("invoice_no").alias("OPM"),
    avg("Is_Return").alias("Rate of return"),
    avg("Total_Amount").alias("Average transaction size")) \
    .select("window","OPM","Total volume of sales","Average transaction size","Rate of return")

# Writing Time Based KPI's to the Json File
TimeBasedKPI = ByTimeData \
    .writeStream \
    .format('json') \
    .outputMode("append") \
    .option("truncate","false") \
    .option("path","./time_kpi/time_kpi_v1/") \
    .option("checkpointLocation","./time_kpi/time_kpi_v1/checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()

# Calculating Time and Country Based KPI's
ByTimeAndCountryData = orderData \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute","1 minute"),"country") \
    .agg(sum("Total_Amount").alias("Total volume of sales"),
    count("invoice_no").alias("OPM"),
    avg("Is_Return").alias("Rate of return")) \
    .select("window","country","OPM","Total volume of sales","Rate of return")

# Writing Time and Country Based KPI's to the Json File
TimeAndCountryBasedKPI = ByTimeAndCountryData \
    .writeStream \
    .format('json') \
    .outputMode("append") \
    .option("truncate","false") \
    .option("path","./country_kpi/country_kpi_v1/") \
    .option("checkpointLocation","./country_kpi/country_kpi_v1/checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()

# Waitig for process to complete
TimeAndCountryBasedKPI.awaitTermination()

#end
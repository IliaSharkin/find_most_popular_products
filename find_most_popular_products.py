import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.master("local").appName("test_work").getOrCreate()

customer_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("joinDate", TimestampType(), True),
    StructField("status", StringType(), True),
])
customers = spark.read.option('delimiter', '\t').csv('customer.csv', schema=customer_schema,  header=False).alias("customer")

product_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("numberOfProducts", IntegerType(), True),
])
products: DataFrame = spark.read.option('delimiter', '\t').csv('product.csv', schema=product_schema,  header=False).alias("product")

order_schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("orderID", IntegerType(), True),
    StructField("productID", IntegerType(), True),
    StructField("numberOfProduct", IntegerType(), True),
    StructField("orderDate", TimestampType(), True),
    StructField("status", StringType(), True),
])
orders: DataFrame = spark.read.option('delimiter', '\t').csv('order.csv', schema=order_schema,  header=False).alias("order")

joined_df = orders.where("status = 'delivered'")\
    .join(products, on=products['product.id'] == orders['productID'])\
    .join(customers, on=customers['customer.id'] == orders['customerID'])\
    .select(["customer.name", "product.name", "order.numberOfProduct"])
    
sum_number_of_product = joined_df.groupBy("customer.name", "product.name")\
    .agg(F.sum("order.numberOfProduct").alias("numberOfProduct_sum"))   
    
window = Window().partitionBy(["customer.name"]).orderBy([F.desc("numberOfProduct_sum"), F.asc("product.name")])

most_populer_poducts_by_customer = sum_number_of_product.withColumn("row_number", F.row_number().over(window))\
    .where("row_number = 1")\
    .select("customer.name", "product.name")
    
most_populer_poducts_by_customer\
    .selectExpr("customer.name as customer_name", "product.name as product_name")\
    .write\
    .option("header", False)\
    .option("sep", "\t")\
    .csv('./results')

# При одинковой популярности товара в выходную таблицу попадёт товар младше по алфавиту.
# Если customer не делал заказов он не попадёт в выходную таблицу.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.streaming import *
from pyspark.sql.streaming import *
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

KAFKA_BROKER = "pathdp3.field.hortonworks.com:6667"
KAFKA_TOPIC = "cryptocurrency-market-data"

# satoriPayload schema

SATORI_SCHEMA= StructType()\
    .add("exchange",StringType())\
    .add("cryptocurrency",StringType())\
    .add("basecurrency",StringType())\
    .add("type",StringType())\
    .add("price",DoubleType())\
    .add("size",DoubleType())\
    .add("bid",DoubleType())\
    .add("ask",DoubleType())\
    .add("open",DoubleType())\
    .add("high",DoubleType())\
    .add("low",DoubleType())\
    .add("volume",DoubleType())\
    .add("timestamp",DateType())

spark = SparkSession \
    .builder \
    .appName("CryptoStreaming") \
    .master("local[2]")\
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to
# kafka broker

messages = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BROKER)\
    .option("subscribe", KAFKA_TOPIC)\
    .option("value.deserializer","org.common.serialization.StringSerializer")\
    .option("key.deserializer",
            "org.common.serialization.ByteArrayDeserializer")\
    .load()
# .schema(SATORI_SCHEMA)\

df = messages.selectExpr("CAST(key AS STRING)",\
                         "CAST(value AS STRING) as ""satori_data")\
    .select(from_json("satori_data",SATORI_SCHEMA).alias("satori_data"))

df.printSchema()

df.writeStream\
    .outputMode("update")\
    .format("console")\
    .option("truncate", False) \
    .option("value.deserializer",
            "org.common.serialization.StringDeSerializer") \
    .option("key.deserializer",
            "org.common.serialization.ByteArrayDeserializer")\
    .start()

# application is failing with Caused by: java.lang.ClassNotFoundException:#
# org.apache.kafka.common.serialization.ByteArrayDeserializer. Added several
# dependencies to no avail. Leaving this alone until the API is stabilized




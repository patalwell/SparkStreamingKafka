from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.streaming import *
from pyspark.sql.streaming import *
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

# Note: When running this app with spark-submit you need the following
# packages: --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
# org.apache.spark:spark-streaming-kafka-0-10-assembly_2.10:2.2.0
# org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0

KAFKA_BROKER = "pathdp3.field.hortonworks.com:6667"
KAFKA_TOPIC = "cryptocurrency-nifi-data"

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
    .option("startingOffsets", "earliest")\
    .option("value.deserializer",
            "org.common.serialization.StringDeserializer")\
    .option("key.deserializer",
            "org.common.serialization.StringDeserializer")\
    .load()

df = messages.selectExpr("CAST(value AS STRING)")

print "========= This is the DataType of Kafka Value ========="
print(type(df))

print "======== This is the schema of the payload ========="
df.printSchema()

rawQuery = df.writeStream\
    .format("memory")\
    .option("truncate", False) \
    .queryName("Payload")\
    .start()

spark.sql("select * from Payload").show()



# application is failing with Caused by: java.lang.ClassNotFoundException:#
# org.apache.kafka.common.serialization.ByteArrayDeserializer. Added several
# dependencies to no avail. Leaving this alone until the API is stabilized




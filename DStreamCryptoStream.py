from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import *
from pyspark.sql.types import *
import json

MASTER = "local[*]"
APP_NAME = "DStreamCryptoStream"
KAFKA_BROKER = "pathdp3.field.hortonworks.com:6667"
KAFKA_TOPIC = ['cryptocurrency-nifi-data']

sc = SparkContext(MASTER, APP_NAME)
ssc = StreamingContext(sc, 1)

# Instantiate our DirectStream with the KafkaUtils class and subsequent method
# Parameters include StreamingContext, Topics, KafkaParameters
# To set the stream to capture the earliest data use:

kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,topics=KAFKA_TOPIC
            ,kafkaParams={"metadata.broker.list":KAFKA_BROKER,
                          "startingOffsets":"earliest"})

# The data from Kafka is returned as a tuple (Key, Value). So we'll want to
# map the data and extract the value from the tuple
value = kafkaStream.map(lambda line: line[1])

print value

# print type(value)
# <class 'pyspark.streaming.kafka.KafkaTransformedDStream'>


# Lazily instantiated global instance of SparkSession (This is a hack to grab
#  sql context)
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

# DataFrame operations inside your streaming program
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to JSON DataFrame by casting the schema
        # data = spark.read.json(rdd, schema=schema)
        raw_data = spark.read.json(rdd)
        clean_data = raw_data.fillna("0")

        # Cast data types within DF
        df = clean_data\
            .withColumn("price",clean_data["price"].cast(FloatType()))\
            .withColumn("size",clean_data["size"].cast(FloatType()))\
            .withColumn("bid",clean_data["bid"].cast(FloatType()))\
            .withColumn("ask",clean_data["ask"].cast(FloatType()))\
            .withColumn("open",clean_data["open"].cast(FloatType()))\
            .withColumn("high",clean_data["high"].cast(FloatType()))\
            .withColumn("low",clean_data["low"].cast(FloatType()))\
            .withColumn("volume",clean_data["volume"].cast(FloatType()))\
            .withColumn("timestamp",clean_data["timestamp"].cast(DateType()))

        # # Check the explicitly mapped schema
        # df.printSchema()

        # Create a tempView so edits can be made in SQL
        df.createOrReplaceTempView("CryptoCurrency")

        print "====== Running Statistics of CryptoCurrency ======="
        spark.sql("SELECT cryptocurrency"
                  ", avg(price) as average_price"
                  ", max(price) as max_price"
                  ", min(price) as min_price"
                  ", std(price) as stnd_dev "
                  "FROM CryptoCurrency "
                  "WHERE cryptocurrency =='ADX' "
                  "OR cryptocurrency == 'BTC' "
                  "OR cryptocurrency == 'ETH' "
                  "GROUP BY cryptocurrency "
                  "ORDER BY cryptocurrency").show()
    except:
        pass

# conduct a functional transformation on our DStreams object value; we are
# inserting our def process function here
value.foreachRDD(process)

# start our computations and stop when the user has issued a keyboard command
ssc.start()
ssc.awaitTermination()

# To Do:
# research what Spark documentation means by "hackery"
# write this application in Java
# insert logging for debugging issues
# insert pausing for debugging, this is delivered too fast to the console


# Methods/Schema under question; cannot seem to map schema on creation of
# dataFrame which will lead to a full table scan!

# rdd = sc.textFile(
#     "/Users/pnalwell/development/druid-satori-demo/utilities/output.json",
#     use_unicode=False)

# mapped_fields = data.rdd.map(lambda l: Row(
#     exchange=str(l[0])
#     ,cryptocurrency=str([1])
#     ,basecurrency=str(l[2])
#     ,type=str(l[3])
#     ,price=float(l[4])
#     ,size=str(l[5])
#     ,bid=str(l[6])
#     ,ask=str(l[7])
#     ,open=str(l[8])
#     ,high=str(l[9])
#     ,low=str(l[10])
#     ,volume=str(l[11])
#     ,timestamp=str(l[12])
# ))

# Specify the Schema for the JSON payload
# schema = StructType([
#     StructField('exchange', StringType())
#     , StructField('cryptocurrency', StringType())
#     , StructField('basecurrency', StringType())
#     , StructField('type', StringType())
#     , StructField('price', FloatType())
#     , StructField('size', FloatType())
#     , StructField('bid', FloatType())
#     , StructField('ask', FloatType())
#     , StructField('open', FloatType())
#     , StructField('high', FloatType())
#     , StructField('low', FloatType())
#     , StructField('volume', FloatType())
#     , StructField('timestamp', FloatType())
#     ])

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import *
from pyspark.sql.types import *

""" Note: When running this app with spark-submit you need the following
spark-submit --packages
org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.2.0
{appName.py}
"""

MASTER = "local[*]"
APP_NAME = "DStreamCryptoStream"
KAFKA_BROKER = "pathdf3.field.hortonworks.com:6667"
KAFKA_TOPIC = ['cryptocurrency-nifi-data']
BATCH_INTERVAL = 10
OFFSET = "earliest"

#constructor for sparkContext; enables spark core
sc = SparkContext(MASTER, APP_NAME)

# constructor accepts SparkContext and Duration in Seconds
# e.g. JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
# Durations.seconds(10));
ssc = StreamingContext(sc, BATCH_INTERVAL)

# Instantiate our DirectStream with the KafkaUtils class and subsequent method
# Parameters include StreamingContext, Topics, KafkaParameters
# To set the stream to capture the earliest data use:
kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,topics=KAFKA_TOPIC
            ,kafkaParams={"metadata.broker.list":KAFKA_BROKER,
                          "startingOffsets":OFFSET})

# The data from Kafka is returned as a tuple (Key, Value). So we'll want to
# transform the DStream with a mappper and point to the value portion of our tuple
value = kafkaStream.map(lambda line: line[1])

#value is now a Dstream object
print value

# print type(value)
# <class 'pyspark.streaming.kafka.KafkaTransformedDStream'>


# Lazily instantiated global instance of SparkSession (This is a hack to grab
# sql context, we need to create a SparkSession using the SparkContext that the
# Streaming conext is using. The method enables this during restart of the
# driver.
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

        schema = StructType([
            StructField('exchange', StringType())
            , StructField('cryptocurrency', StringType())
            , StructField('basecurrency', StringType())
            , StructField('type', StringType())
            , StructField('price', DoubleType())
            , StructField('size', DoubleType())
            , StructField('bid', DoubleType())
            , StructField('ask', DoubleType())
            , StructField('open', DoubleType())
            , StructField('high', DoubleType())
            , StructField('low', DoubleType())
            , StructField('volume', DoubleType())
            , StructField('timestamp', LongType())
        ])

        # Convert RDD[String] to JSON DataFrame by casting the schema
        data = spark.read.json(rdd, schema=schema)
        # data.show()
        # drop null values from our aggregations
        df = data.na.drop()

        # Check the explicitly mapped schema
        # df.printSchema()

        #Create a tempView so edits can be made in SQL
        df.createOrReplaceTempView("CryptoCurrency")

        # #Show All DataSets for USD
        # spark.sql("""
        # SELECT *
        # FROM CryptoCurrency
        # WHERE basecurrency == 'USD'
        # """).show()

        # print "**** Price per transaction ****"
        # spark.sql("""
        # SELECT cryptocurrency
        # ,price
        # ,basecurrency
        # FROM CryptoCurrency
        # WHERE (cryptocurrency == 'BTC'
        # OR cryptocurrency == 'ETH')
        # AND basecurrency == 'USD'""").show()

        # Get avg, max, min, and stdev for BTC, ETH, LTC, and ALX
        # we need to normalize our standard deviation by dividing by our
        # price average in order to calculate a per transaction
        # normalized_stnd_dev closer to 0 is less volatile
        print "**** Running Statistics of CryptoCurrency ****"
        spark.sql("""
        SELECT cryptocurrency
        ,avg(price) - std(price) as lower_1_std_bound
        ,avg(price) as average_price
        ,avg(price) + std(price) as upper_1_std_bound
        ,max(price) as max_price
        ,min(price) as min_price
        ,std(price) as 1_std
        ,std(price) * 2 as 2_std
        ,std(price)/avg(price)*100 as normalized_stnd_dev
        FROM CryptoCurrency
        WHERE (cryptocurrency =='ADX'
        OR cryptocurrency == 'BTC'
        OR cryptocurrency == 'LTC'
        OR cryptocurrency == 'ETH')
        AND basecurrency == 'USD'
        GROUP BY cryptocurrency
        ORDER BY cryptocurrency""").show()

    except:
        pass

# conduct an operation on our DStreams object; we are
# inserting our def process function here and applying said function to each
# RDD generated in the stream
value.foreachRDD(process)

# start our computations and stop when the user has issued a keyboard command
ssc.start()
ssc.awaitTermination()


"""
To Do:

1. Research what Spark documentation means by "hackery"
2. Write this application in Java
3. Insert logging for debugging issues

Methods/Schema under question; cannot seem to map schema on creation of
dataFrame which will lead to a full table scan!

Update: Looks like the data needs to be typeCasted prior to entry into spark;
particularly with schemaLess payloads like JSON. You can use nifi or custom
scripts for this process prior to entry into Kafka.

Object Assets for Application Testing are below:
"""

# rdd = sc.textFile(
#     "/Users/pnalwell/development/druid-satori-demo/utilities/output.json",
#     use_unicode=False)

# mapped_fields = data.rdd.map(lambda l: Row(
#     exchange=str(l[0])
#     ,cryptocurrency=str([1])
#     ,basecurrency=str(l[2])
#     ,type=str(l[3])
#     ,price=double(l[4])
#     ,size=double(l[5])
#     ,bid=double(l[6])
#     ,ask=double(l[7])
#     ,open=double(l[8])
#     ,high=double(l[9])
#     ,low=double(l[10])
#     ,volume=double(l[11])
#     ,timestamp=long(l[12])
# ))

# # Cast data types within DF
# df = clean_data\
#     .withColumn("price",clean_data["price"].cast(FloatType()))\
#     .withColumn("size",clean_data["size"].cast(FloatType()))\
#     .withColumn("bid",clean_data["bid"].cast(FloatType()))\
#     .withColumn("ask",clean_data["ask"].cast(FloatType()))\
#     .withColumn("open",clean_data["open"].cast(FloatType()))\
#     .withColumn("high",clean_data["high"].cast(FloatType()))\
#     .withColumn("low",clean_data["low"].cast(FloatType()))\
#     .withColumn("volume",clean_data["volume"].cast(FloatType()))\
#     .withColumn("timestamp",clean_data["timestamp"].cast(DateType()))

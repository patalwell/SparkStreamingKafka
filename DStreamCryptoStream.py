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
                          "auto.offset.reset":"smallest"})

# The data from Kafka is returned as a tuple (Key, Value). So we'll want to
# map the data and extract the value from the tuple
value = kafkaStream.map(lambda line: json.dumps(line[1]))


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

        # Specify the Schema for the JSON payload
        # Boolean indicates if Null is acceptable, in this case we don't want
        #  null values
        schema = StructType([
            StructField('exchange', StringType(), False)
            , StructField('cryptocurrency', StringType(), False)
            , StructField('basecurrency', StringType(), False)
            , StructField('type', StringType(), False)
            , StructField('price', FloatType(),False)
            , StructField('size', FloatType(),False)
            , StructField('bid', FloatType(), False)
            , StructField('ask', FloatType(), False)
            , StructField('open', FloatType(), False)
            , StructField('high', FloatType(), False)
            , StructField('low', FloatType(), False)
            , StructField('volume', FloatType(), False)
            , StructField('timestamp', FloatType(), False)
            ])

        # Convert RDD[String] to JSON DataFrame by casting the schema
        data = spark.read.json(rdd, schema=schema)

        print "========= This is the Schema: Notice the correct types for " \
              "aggregations ========="
        data.printSchema()

        print "======== This is the full dataframe ========"
        data.select('*').show()

        # Create a tempview so edits can be made in SQL
        data.createOrReplaceTempView("CryptoCurrency")

        print "====== This is a running count of popular exchanges ======="
        spark.sql("SELECT exchange, count(*) from CryptoCurrency GROUP BY "
                  "exchange").show()
    except:
        pass

value.foreachRDD(process)

ssc.start()
ssc.awaitTermination()

# To Do:
# Offset Kafka with Earliest Data...what is the proper key:value argument?
# research what Spark documentation means by "hackery"
# write this application in Java
# insert logging for debugging issues
# insert pausing for debugging, this is delivered too fast to the console

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import *
from pyspark.sql.types import *

master = "local[*]"
appName = "DStreamCryptoStream"
KAFKA_BROKER = "pathdp3.field.hortonworks.com:6667"
KAFKA_TOPIC = ['cryptocurrency-nifi-data']

sc = SparkContext(master, appName)
ssc = StreamingContext(sc, 1)

# Instantiate our DirectStream with the KafkaUtils class and subsequent method
# Parameters include StreamingContext, Topics, KafkaParameters
# To set the stream to capture the earliest data use:

kafkaStream = KafkaUtils.createDirectStream(ssc=ssc,topics=KAFKA_TOPIC
            ,kafkaParams={"metadata.broker.list":KAFKA_BROKER,
                          "auto.offset.reset":"smallest"})

# The data from Kafka is returned as a tuple (Key, Value). So we'll want to
# map the data and extract the value from the tuple
value = kafkaStream.map(lambda line: line[1])


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
        schema = StructType([
            StructField('exchange', StringType())
            , StructField('cryptocurrency', StringType())
            , StructField('basecurrency', StringType())
            , StructField('type', StringType())
            , StructField('price', FloatType())
            , StructField('size', FloatType())
            , StructField('bid', FloatType())
            , StructField('ask', FloatType())
            , StructField('open', FloatType())
            , StructField('high', FloatType())
            , StructField('low', FloatType())
            , StructField('volume', FloatType())
            , StructField('timestamp', FloatType())
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
        # insert logging for debugging issues
        pass

value.foreachRDD(process)

ssc.start()
ssc.awaitTermination()

# To Do:
# Offset Kafka with Earliest Data
# research hackery
# shim this application in Java

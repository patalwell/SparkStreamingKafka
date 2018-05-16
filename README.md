<h1>Spark Streaming and Structured Streaming with PySpark and Kafka</h1>

<h3>Background:</h3>
This repository contains two applications CryptoStream.py and DStreamCryptoStream.py. The former is using Spark's Structured Streaming API while the latter is using Spark's Spark Streaming API. There are dependency issues with the Structured Streaming application, however the Structured Streaming Python API is still experimental with Kafka 10
. I ran into the same error with a different package, namely Kafka 8, but have yet to research potential issues beyond scape goating the class dependencies bundled into the jars. The DStreamCryptoStream.py application works as intended with Kafka 8 libraries.

<h3>Current Issues:</h3>

1. CryptoStream.py is failing with 

      Caused by: java.lang.ClassNotFoundException: org.apache.kafka.common.serialization.ByteArrayDeserializer. 

      I added several dependencies to no avail. Leaving this alone until the API is stabilized

<h3>To Do:</h3>

1. Insert logging for debugging issues
2. Create an ML model for VaR
3. Create a layer of persistence with Pheonix/HBase


<h3>Sample Results:</h3>

+--------------+-----------------+---------+---------+
|cryptocurrency|    average_price|max_price|min_price|
+--------------+-----------------+---------+---------+
|           BTC|8311.699999999999|   8390.1|   8270.0|
|           ETH|697.9666666666666|   704.82|   693.71|
|           LTC|           137.97|    138.1|   137.84|
+--------------+-----------------+---------+---------+

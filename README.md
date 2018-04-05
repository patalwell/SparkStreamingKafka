<h1>Spark Streaming and Structured Streaming with PySpark and Kafka</h1>

<h3>Background:</h3>
This repository contains two applications CryptoStream.py and DStreamCryptoStream.py. The former is using Spark's Structured Streaming API while the latter is using Spark's Spark Streaming API. There are depenency issues with the first application, however the Structured Streaming API is still experimental with Kafka 10. I still ran into the same error with a different package, namely Kafka 8, but have yet to research potential issues beyond scape goating the class dependecies bundleded into the jars. The DStreamCryptoStream.py application works as intended with Kafka 8 libraries.

<h3>Current Issues:</h3>

1. CryptoStream.py is failing with 

      Caused by: java.lang.ClassNotFoundException: org.apache.kafka.common.serialization.ByteArrayDeserializer. 

      I added several dependencies to no avail. Leaving this alone until the API is stabilized

2. DStreamCryptoStream.py is conducting a full table scan prior to mapping explicit data types to columns. I've tried to circumnavigate the limitations of the createDataFrame() parameters e.g. unicode and Row(); but couldn't find a solid means to extracting complex tolkens from the JSON payload to string.

<h3>To Do:</h3>

1. Research what Spark documentation means by "hackery"
2. Write this application in Java
3. Insert logging for debugging issues
4. Insert pausing for debugging, this is delivered too fast to the console
5. Methods/Schema under question; cannot seem to map schema on creation of dataFrame which will lead to a full table scan!

<h3>Sample Results:</h3>

+--------------+-------------------+---------+---------+-------------------+-------------------+
|cryptocurrency|      average_price|max_price|min_price|           stnd_dev|           variance|
+--------------+-------------------+---------+---------+-------------------+-------------------+
|           ADX|0.12300236678720466| 0.607375| 8.422E-5|0.23580329934575558|0.05560319598234401|
|           BTC| 30191.269882171073| 412000.0|   5284.9|  92177.95594861188|8.496775562864232E9|
|           ETH| 3178.8769934739516|  45002.0|0.0550548| 10845.304558624572|1.176206309693229E8|
+--------------+-------------------+---------+---------+-------------------+-------------------+
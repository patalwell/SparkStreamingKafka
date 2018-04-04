<h1>Spark Streaming and Structured Streaming with PySpark and Kafka</h1>

<h3>Background:</h3>
I was trying to familiarize myself with Spark's streaming libraries; but wound up running into dependency issues as it relates to Kafka 10 and Pyspark Structured Streaming, which is still experimental. The spark streaming application contains logic that grabs a kafka topic and applies DF transfomrations in order to create a data frame, cast a schema, and derive statistical values in realtime.

<h3>Current Issues:</h3>


<h3>Sample Results:</h3>

+--------------+-------------------+---------+---------+-------------------+-------------------+
|cryptocurrency|      average_price|max_price|min_price|           stnd_dev|           variance|
+--------------+-------------------+---------+---------+-------------------+-------------------+
|           ADX|0.12300236678720466| 0.607375| 8.422E-5|0.23580329934575558|0.05560319598234401|
|           BTC| 30191.269882171073| 412000.0|   5284.9|  92177.95594861188|8.496775562864232E9|
|           ETH| 3178.8769934739516|  45002.0|0.0550548| 10845.304558624572|1.176206309693229E8|
+--------------+-------------------+---------+---------+-------------------+-------------------+
